package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	gitdrs "github.com/calypr/git-drs/client"
	loomingest "github.com/calypr/loom/client/ingest"
)

func TestParseInputAndValidateSelection(t *testing.T) {
	commit := strings.Repeat("a", 40)
	raw := fmt.Sprintf(`{"projectId":"org-with-dash-project-name","organization":"org-with-dash","project":"project-name","repository":"https://github.com/calypr/meta.git","commit":%q,"files":["META/Patient.ndjson"]}`, commit)
	input, err := parseInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := validateInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Owner != "calypr" || repository.Name != "meta" || repository.Canonical != "https://github.com/calypr/meta" {
		t.Fatalf("unexpected normalized repository: %#v", repository)
	}

	if _, err := parseInput(raw + `{}`); err == nil {
		t.Fatal("expected trailing JSON to be rejected")
	}
	input.Files = []string{"META/Patient.ndjson", "META/Patient.ndjson"}
	if err := validateMetadataPaths(input.Files); err == nil {
		t.Fatal("expected duplicate selection to be rejected")
	}
	input.Files = []string{"META/../Patient.ndjson"}
	if err := validateMetadataPaths(input.Files); err == nil {
		t.Fatal("expected traversal path to be rejected")
	}
}

func TestParseLFSPointerIsStrict(t *testing.T) {
	valid := fmt.Sprintf("%s\noid sha256:%064x\nsize 17\n", lfsPointerVersion, 42)
	file, ok, err := gitdrs.ParseLFSPointer([]byte(valid))
	if err != nil || !ok {
		t.Fatalf("parse valid pointer: ok=%t err=%v", ok, err)
	}
	if file.OID != fmt.Sprintf("sha256:%064x", 42) || file.Size != 17 {
		t.Fatalf("unexpected pointer: %#v", file)
	}
	for _, invalid := range []string{
		strings.Replace(valid, "size 17", "size -1", 1),
		strings.Replace(valid, "size 17", "size 017", 1),
		strings.Replace(valid, "size 17", "size 17\next-0-foo bar", 1),
		strings.Replace(valid, "\n", "\r\n", 1),
		strings.Replace(valid, fmt.Sprintf("%064x", 42), "not-a-hash", 1),
	} {
		if _, _, err := gitdrs.ParseLFSPointer([]byte(invalid)); err == nil {
			t.Fatalf("expected malformed pointer to be rejected: %q", invalid)
		}
	}
	if _, ok, err := gitdrs.ParseLFSPointer([]byte(`{"resourceType":"Patient"}`)); err != nil || ok {
		t.Fatalf("ordinary NDJSON should not be treated as a pointer: ok=%t err=%v", ok, err)
	}
}

func TestRunJobFetchesPinnedFilesHydratesAndPublishesReceipt(t *testing.T) {
	commit := strings.Repeat("b", 40)
	patient := `{"resourceType":"Patient","id":"1"}`
	pointer := fmt.Sprintf("%s\noid sha256:%064x\nsize %d\n", lfsPointerVersion, 99, len(patient))
	contents := map[string]string{
		"/api/v3/repos/calypr/meta/contents/META/Patient.ndjson":  pointer,
		"/api/v3/repos/calypr/meta/contents/META/Specimen.ndjson": "",
	}
	githubServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Query().Get("ref") != commit {
			t.Errorf("unexpected GitHub request: %s %s", request.Method, request.URL.String())
		}
		if request.Header.Get("Authorization") != "Bearer github-secret" {
			t.Errorf("GitHub token was not sent in Authorization header")
		}
		if request.Header.Get("Accept") != "application/vnd.github.raw+json" {
			t.Errorf("unexpected Accept header %q", request.Header.Get("Accept"))
		}
		content, exists := contents[request.URL.Path]
		if !exists {
			http.NotFound(writer, request)
			return
		}
		_, _ = io.WriteString(writer, content)
	}))
	defer githubServer.Close()

	github, err := newGitHubSourceAt(githubServer.URL, "github-secret", githubServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	drs := &recordingDRS{content: []byte(patient)}
	publishing := &recordingPublisher{receipt: loomingest.Receipt{
		OperationID: "funnel-task-123", SourceRevision: 7, ManifestHash: "manifest-digest",
		ExecutionID: "execution-456", State: "PUBLISHED", Reused: true,
	}}
	input := inputData{
		ProjectID: "org-with-dash-project-name", Organization: "org-with-dash", Project: "project-name",
		Repository: "https://github.com/calypr/meta.git", Commit: commit,
		Files: []string{"META/Specimen.ndjson", "META/Patient.ndjson"},
	}
	config := jobConfig{operationID: "funnel-task-123", workspaceDir: t.TempDir(), maxFileBytes: maxFileBytes}
	receipt, err := runJob(context.Background(), config, input, workerDependencies{github: github, drs: drs, publisher: publishing})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateReceipt(receipt, config.operationID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(drs.paths, []string{"META/Patient.ndjson"}) || drs.oid != fmt.Sprintf("sha256:%064x", 99) || drs.size != int64(len(patient)) || !drs.overwrite {
		t.Fatalf("unexpected Git-DRS pull: %#v", drs)
	}
	if publishing.request.Project != input.ProjectID || publishing.request.AuthResourcePath != "/programs/org-with-dash/projects/project-name" || publishing.request.OperationID != config.operationID {
		t.Fatalf("unexpected Loom request identity: %#v", publishing.request)
	}
	wantPaths := []string{"META/Patient.ndjson", "META/Specimen.ndjson"}
	gotPaths := make([]string, len(publishing.request.Manifest.Files))
	for index, file := range publishing.request.Manifest.Files {
		gotPaths[index] = file.Path
	}
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("manifest paths = %v, want %v", gotPaths, wantPaths)
	}
	if publishing.request.Manifest.Repository != "https://github.com/calypr/meta" || publishing.request.Manifest.Commit != commit {
		t.Fatalf("unexpected source manifest: %#v", publishing.request.Manifest)
	}
	if !reflect.DeepEqual(publishing.fileContents, map[string]string{
		"META/Patient.ndjson":  patient,
		"META/Specimen.ndjson": "",
	}) {
		t.Fatalf("published bytes do not match the selected hydrated files: %#v", publishing.fileContents)
	}
	entries, err := os.ReadDir(config.workspaceDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("private staging workspace was not cleaned: %v", entries)
	}
}

func TestFetchAndHydrateAcceptsMaximumLFSSizeWithoutCountingPointerText(t *testing.T) {
	const maximum = int64(60 << 20)
	commit := strings.Repeat("e", 40)
	pointer := fmt.Sprintf("%s\noid sha256:%064x\nsize %d\n", lfsPointerVersion, 7, maximum)
	github := newContentsSource(t, commit, map[string]string{"/api/v3/repos/calypr/meta/contents/META/Patient.ndjson": pointer})
	repository, err := parseGitHubRepository("https://github.com/calypr/meta")
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	drs := &sentinelDRS{err: errors.New("reached DRS pull")}
	_, err = fetchAndHydrateFileWithLimits(context.Background(), workspace, commit, "META/Patient.ndjson", repository, github, drs, sourceLimits{maxFileBytes: maximum})
	if !errors.Is(err, drs.err) {
		t.Fatalf("expected to reach Git-DRS at the exact payload limit, got %v", err)
	}
	if !drs.called || len(drs.files) != 1 || drs.files[0].Path != "META/Patient.ndjson" || drs.files[0].Size != maximum {
		t.Fatalf("unexpected Git-DRS request at payload limit: %#v", drs)
	}
}

func TestFetchAndHydrateEnforcesPerFileLimitWithoutDatasetBudget(t *testing.T) {
	commit := strings.Repeat("f", 40)
	zeroPointer := fmt.Sprintf("%s\noid sha256:%064x\nsize 0\n", lfsPointerVersion, 9)
	github := newContentsSource(t, commit, map[string]string{
		"/api/v3/repos/calypr/meta/contents/META/Group.ndjson":   "data",
		"/api/v3/repos/calypr/meta/contents/META/Patient.ndjson": zeroPointer,
	})
	repository, err := parseGitHubRepository("https://github.com/calypr/meta")
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	groupPath, err := fetchAndHydrateFileWithLimits(context.Background(), workspace, commit, "META/Group.ndjson", repository, github, &recordingDRS{}, sourceLimits{maxFileBytes: 4})
	if err != nil {
		t.Fatalf("fetch per-file-limit raw file: %v", err)
	}
	if info, err := os.Stat(groupPath); err != nil || info.Size() != 4 {
		t.Fatalf("raw file after hydration: info=%v err=%v", info, err)
	}
	if err := os.Remove(groupPath); err != nil {
		t.Fatal(err)
	}
	zeroDRS := &recordingDRS{}
	patientPath, err := fetchAndHydrateFileWithLimits(context.Background(), workspace, commit, "META/Patient.ndjson", repository, github, zeroDRS, sourceLimits{maxFileBytes: 4})
	if err != nil {
		t.Fatalf("fetch empty LFS file: %v", err)
	}
	if info, err := os.Stat(patientPath); err != nil || info.Size() != 0 {
		t.Fatalf("empty LFS file after hydration: info=%v err=%v", info, err)
	}
	if !zeroDRS.overwrite || len(zeroDRS.paths) != 1 || zeroDRS.paths[0] != "META/Patient.ndjson" || zeroDRS.size != 0 {
		t.Fatalf("unexpected empty LFS pull: %#v", zeroDRS)
	}
}

func TestRunJobAllowsSelectedFilesWhoseTotalExceedsPerFileLimit(t *testing.T) {
	commit := strings.Repeat("a", 40)
	github := newContentsSource(t, commit, map[string]string{
		"/api/v3/repos/calypr/meta/contents/META/Patient.ndjson":  "1234",
		"/api/v3/repos/calypr/meta/contents/META/Specimen.ndjson": "5678",
	})
	input := inputData{
		ProjectID: "org-project", Organization: "org", Project: "project",
		Repository: "https://github.com/calypr/meta", Commit: commit,
		Files: []string{"META/Patient.ndjson", "META/Specimen.ndjson"},
	}
	publishing := &recordingPublisher{receipt: loomingest.Receipt{
		OperationID: "task-large-dataset", SourceRevision: 3, ManifestHash: "hash", ExecutionID: "execution", State: "PUBLISHED",
	}}
	config := jobConfig{operationID: "task-large-dataset", workspaceDir: t.TempDir(), maxFileBytes: 4}
	_, err := runJob(context.Background(), config, input, workerDependencies{github: github, drs: &recordingDRS{}, publisher: publishing})
	if err != nil {
		t.Fatalf("runJob() error = %v; two 4-byte files should exceed no shared 4-byte dataset cap", err)
	}
	if !reflect.DeepEqual(publishing.fileContents, map[string]string{
		"META/Patient.ndjson":  "1234",
		"META/Specimen.ndjson": "5678",
	}) {
		t.Fatalf("hydrated file contents = %#v", publishing.fileContents)
	}
}

func TestConfiguredMaxFileBytesCanReduceDefaultLimit(t *testing.T) {
	if got, err := configuredMaxFileBytes(func(string) string { return "" }); err != nil || got != maxFileBytes {
		t.Fatalf("default max file bytes = %d, err=%v", got, err)
	}
	if got, err := configuredMaxFileBytes(func(key string) string {
		if key == "ETL_MAX_FILE_BYTES" {
			return "1024"
		}
		return ""
	}); err != nil || got != 1024 {
		t.Fatalf("configured max file bytes = %d, err=%v", got, err)
	}
	for _, value := range []string{"-1", "0", "not-a-number", fmt.Sprint(maxFileBytes + 1)} {
		if _, err := configuredMaxFileBytes(func(string) string { return value }); err == nil {
			t.Fatalf("ETL_MAX_FILE_BYTES=%q accepted", value)
		}
	}
}

func TestValidateReceiptRequiresPublishedState(t *testing.T) {
	if err := validateReceipt(loomingest.Receipt{OperationID: "task", State: "READY", ManifestHash: "x", ExecutionID: "y"}, "task"); err == nil {
		t.Fatal("expected non-published receipt state to fail")
	}
}

func TestGitHubFetchEnforcesLimitAndDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		redirected = true
		if request.Header.Get("Authorization") != "" {
			t.Error("GitHub token was forwarded to a redirect target")
		}
	}))
	defer target.Close()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusFound)
	}))
	defer server.Close()
	source, err := newGitHubSourceAt(server.URL, "github-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	repository, _ := parseGitHubRepository("https://github.com/calypr/meta")
	_, err = source.fetchPinned(context.Background(), repository, strings.Repeat("d", 40), "META/Patient.ndjson", filepath.Join(t.TempDir(), "Patient.ndjson"), 32)
	if err == nil || !strings.Contains(err.Error(), "HTTP 302") {
		t.Fatalf("expected redirect to be rejected, got %v", err)
	}
	if redirected {
		t.Fatal("GitHub client followed a redirect")
	}

	bodyServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(writer, "four")
	}))
	defer bodyServer.Close()
	source, err = newGitHubSourceAt(bodyServer.URL, "github-secret", bodyServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "META", "Patient.ndjson")
	if _, err := source.fetchPinned(context.Background(), repository, strings.Repeat("d", 40), "META/Patient.ndjson", destination, 3); err == nil {
		t.Fatal("expected oversized GitHub body to be rejected")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("partial oversized file was left in staging: %v", err)
	}
}

type recordingDRS struct {
	content   []byte
	paths     []string
	oid       string
	size      int64
	overwrite bool
}

type sentinelDRS struct {
	called bool
	files  []gitdrs.File
	err    error
}

func (d *sentinelDRS) Pull(_ context.Context, options gitdrs.PullOptions) error {
	d.called = true
	d.files = append([]gitdrs.File(nil), options.Files...)
	return d.err
}

func (d *recordingDRS) Pull(_ context.Context, options gitdrs.PullOptions) error {
	d.overwrite = options.Overwrite
	for _, file := range options.Files {
		d.paths = append(d.paths, file.Path)
		d.oid = file.OID
		d.size = file.Size
		path := filepath.Join(options.Root, filepath.FromSlash(file.Path))
		if err := os.WriteFile(path, d.content, 0o600); err != nil {
			return err
		}
	}
	return nil
}

type recordingPublisher struct {
	receipt      loomingest.Receipt
	request      loomingest.Request
	fileContents map[string]string
}

func (p *recordingPublisher) Publish(ctx context.Context, request loomingest.Request) (loomingest.Receipt, error) {
	p.request = request
	p.fileContents = make(map[string]string, len(request.Manifest.Files))
	for _, selected := range request.Manifest.Files {
		file := loomingest.File{Path: selected.Path}
		for _, requested := range request.Files {
			if requested.Path == selected.Path {
				file = requested
				break
			}
		}
		localPath := file.LocalPath
		var cleanup func()
		if localPath == "" {
			if request.AcquireFile == nil {
				return loomingest.Receipt{}, fmt.Errorf("no local file or acquisition callback for %s", file.Path)
			}
			var err error
			localPath, cleanup, err = request.AcquireFile(ctx, file.Path)
			if err != nil {
				if cleanup != nil {
					cleanup()
				}
				return loomingest.Receipt{}, err
			}
		}
		data, err := os.ReadFile(localPath)
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			return loomingest.Receipt{}, err
		}
		p.fileContents[file.Path] = string(data)
		info, err := os.Stat(localPath)
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			return loomingest.Receipt{}, err
		}
		if info.Mode().Perm() != 0o400 {
			if cleanup != nil {
				cleanup()
			}
			return loomingest.Receipt{}, fmt.Errorf("file %s mode is %o, want read-only 400", file.Path, info.Mode().Perm())
		}
		if cleanup != nil {
			cleanup()
		}
		if _, err := os.Stat(localPath); !os.IsNotExist(err) {
			return loomingest.Receipt{}, fmt.Errorf("file %s cleanup did not remove staged bytes", file.Path)
		}
	}
	return p.receipt, nil
}

func newContentsSource(t *testing.T, commit string, contentByPath map[string]string) *githubSource {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("ref") != commit {
			t.Errorf("GitHub ref = %q, want %q", request.URL.Query().Get("ref"), commit)
		}
		content, ok := contentByPath[request.URL.Path]
		if !ok {
			http.NotFound(writer, request)
			return
		}
		_, _ = io.WriteString(writer, content)
	}))
	t.Cleanup(server.Close)
	source, err := newGitHubSourceAt(server.URL, "github-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return source
}
