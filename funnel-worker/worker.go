package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	gitdrs "github.com/calypr/git-drs/client"
	loomingest "github.com/calypr/loom/client/ingest"
)

const (
	maxMetadataFiles   = 100
	maxInputDataBytes  = 64 << 10
	maxFileBytes       = int64(1 << 30)
	maxLFSPointerBytes = int64(4096)
	jobTimeout         = time.Hour
	lfsPointerVersion  = "version https://git-lfs.github.com/spec/v1"
	githubAPIBaseURL   = "https://api.github.com"
	githubRequestAgent = "calypr-funnel-etl"
)

type inputData struct {
	ProjectID    string   `json:"projectId"`
	Organization string   `json:"organization"`
	Project      string   `json:"project"`
	Repository   string   `json:"repository"`
	Commit       string   `json:"commit"`
	Files        []string `json:"files"`
}

type jobConfig struct {
	accessToken  string
	githubToken  string
	geckoURL     string
	drsURL       string
	loomURL      string
	operationID  string
	workspaceDir string
	maxFileBytes int64
}

type githubRepository struct {
	Owner     string
	Name      string
	Canonical string
}

type workerDependencies struct {
	github    *githubSource
	drs       drsPuller
	publisher publisher
}

type sourceLimits struct {
	maxFileBytes int64
}

type drsPuller interface {
	Pull(context.Context, gitdrs.PullOptions) error
}

type publisher interface {
	Publish(context.Context, loomingest.Request) (loomingest.Receipt, error)
}

func parseInput(raw string) (inputData, error) {
	if len(raw) > maxInputDataBytes {
		return inputData{}, errors.New("INPUT_DATA exceeds the size limit")
	}
	var input inputData
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return inputData{}, fmt.Errorf("decode INPUT_DATA: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return inputData{}, errors.New("INPUT_DATA must contain one JSON object")
	}
	return input, nil
}

func validateInput(input inputData) (githubRepository, error) {
	if !validProjectComponent(input.Organization) || !validProjectComponent(input.Project) {
		return githubRepository{}, errors.New("organization and project must be valid Gen3 path components")
	}
	if input.ProjectID != input.Organization+"-"+input.Project {
		return githubRepository{}, errors.New("projectId must match organization and project")
	}
	if err := validateCommit(input.Commit); err != nil {
		return githubRepository{}, err
	}
	repository, err := parseGitHubRepository(input.Repository)
	if err != nil {
		return githubRepository{}, err
	}
	if err := validateMetadataPaths(input.Files); err != nil {
		return githubRepository{}, err
	}
	return repository, nil
}

func validProjectComponent(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("-_.", char) {
			continue
		}
		return false
	}
	return true
}

func validateCommit(commit string) error {
	if len(commit) != 40 && len(commit) != 64 {
		return errors.New("commit must be a full 40- or 64-character Git SHA")
	}
	for _, char := range commit {
		if (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F') {
			continue
		}
		return errors.New("commit must contain only hexadecimal characters")
	}
	return nil
}

func validateOperationID(value string) error {
	if value == "" || len(value) > 200 || strings.TrimSpace(value) != value {
		return errors.New("ETL_OPERATION_ID must be non-empty, trimmed, and at most 200 characters")
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("-_.:", char) {
			continue
		}
		return errors.New("ETL_OPERATION_ID contains unsupported characters")
	}
	return nil
}

func parseGitHubRepository(raw string) (githubRepository, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "github.com") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return githubRepository{}, errors.New("repository must be an unauthenticated HTTPS GitHub repository URL")
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return githubRepository{}, errors.New("repository URL must use standard HTTPS")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 2 || !validGitHubComponent(parts[0]) {
		return githubRepository{}, errors.New("repository URL must identify one GitHub owner and repository")
	}
	repositoryName := strings.TrimSuffix(parts[1], ".git")
	if !validGitHubComponent(repositoryName) {
		return githubRepository{}, errors.New("repository URL must identify one GitHub owner and repository")
	}
	return githubRepository{
		Owner: parts[0], Name: repositoryName,
		Canonical: "https://github.com/" + parts[0] + "/" + repositoryName,
	}, nil
}

func validGitHubComponent(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 100 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("-_.", char) {
			continue
		}
		return false
	}
	return true
}

func validateMetadataPaths(files []string) error {
	if len(files) == 0 || len(files) > maxMetadataFiles {
		return fmt.Errorf("files must contain between 1 and %d selected metadata paths", maxMetadataFiles)
	}
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		if len(file) > 255 || !strings.HasPrefix(file, "META/") || strings.ContainsAny(file, "\\\x00\r\n\t") || filepath.IsAbs(file) || filepath.ToSlash(filepath.Clean(filepath.FromSlash(file))) != file {
			return fmt.Errorf("file path %q is not a canonical relative metadata path", file)
		}
		name := strings.TrimPrefix(file, "META/")
		if name == "" || strings.Contains(name, "/") || !strings.HasSuffix(name, ".ndjson") || strings.TrimSpace(name) != name {
			return fmt.Errorf("file path %q must be a direct META/*.ndjson path", file)
		}
		stem := strings.TrimSuffix(name, ".ndjson")
		if stem == "" || stem == "." || stem == ".." {
			return fmt.Errorf("file path %q must have a canonical filename", file)
		}
		if stem[0] < 'A' || stem[0] > 'Z' {
			return fmt.Errorf("file path %q must name a canonical FHIR resource type", file)
		}
		for _, char := range stem[1:] {
			if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') {
				continue
			}
			return fmt.Errorf("file path %q must name a canonical FHIR resource type", file)
		}
		if _, exists := seen[file]; exists {
			return fmt.Errorf("files contains duplicate path %q", file)
		}
		seen[file] = struct{}{}
	}
	return nil
}

func validateServiceURL(value, name string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%s must be a trusted HTTP(S) base URL", name)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func runJob(ctx context.Context, config jobConfig, input inputData, deps workerDependencies) (loomingest.Receipt, error) {
	repository, err := validateInput(input)
	if err != nil {
		return loomingest.Receipt{}, err
	}
	if err := validateOperationID(config.operationID); err != nil {
		return loomingest.Receipt{}, err
	}
	if deps.github == nil || deps.drs == nil || deps.publisher == nil {
		return loomingest.Receipt{}, errors.New("publish worker dependencies are incomplete")
	}
	workspace, cleanup, err := createPrivateWorkspace(config.workspaceDir)
	if err != nil {
		return loomingest.Receipt{}, err
	}
	defer cleanup()
	maxFileSize := config.maxFileBytes
	if maxFileSize == 0 {
		maxFileSize = maxFileBytes
	}
	paths := append([]string(nil), input.Files...)
	sort.Strings(paths)
	files := make([]loomingest.File, len(paths))
	manifestFiles := make([]loomingest.ManifestFile, len(paths))
	for index, sourcePath := range paths {
		files[index] = loomingest.File{Path: sourcePath}
		manifestFiles[index] = loomingest.ManifestFile{Path: sourcePath}
	}
	request := loomingest.Request{
		Project:          input.ProjectID,
		AuthResourcePath: fmt.Sprintf("/programs/%s/projects/%s", input.Organization, input.Project),
		OperationID:      config.operationID,
		Manifest: loomingest.Manifest{
			Repository: repository.Canonical,
			Commit:     strings.ToLower(input.Commit),
			Files:      manifestFiles,
		},
		Files: files,
		AcquireFile: func(fileContext context.Context, sourcePath string) (string, func(), error) {
			localPath, err := fetchAndHydrateFileWithLimits(fileContext, workspace, input.Commit, sourcePath, repository, deps.github, deps.drs, sourceLimits{maxFileBytes: maxFileSize})
			if err != nil {
				return "", nil, err
			}
			cleanupFile := func() {
				if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
					slogWarnCleanup(localPath, err)
				}
			}
			if sourcePath == documentReferencePath {
				if err := reconcileStagedDocumentReferences(fileContext, workspace, localPath, input, repository, config, deps, maxFileSize); err != nil {
					cleanupFile()
					return "", nil, fmt.Errorf("reconcile %s: %w", sourcePath, err)
				}
			}
			return localPath, cleanupFile, nil
		},
	}
	receipt, err := deps.publisher.Publish(ctx, request)
	if err != nil {
		return loomingest.Receipt{}, fmt.Errorf("publish selected metadata to Loom: %w", err)
	}
	if err := validateReceipt(receipt, config.operationID); err != nil {
		return loomingest.Receipt{}, err
	}
	return receipt, nil
}

func createPrivateWorkspace(root string) (string, func(), error) {
	if strings.TrimSpace(root) == "" {
		return "", nil, errors.New("ETL_WORKSPACE_DIR is required")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", nil, fmt.Errorf("resolve worker workspace: %w", err)
	}
	if err := os.MkdirAll(absRoot, 0o700); err != nil {
		return "", nil, fmt.Errorf("create worker workspace: %w", err)
	}
	info, err := os.Lstat(absRoot)
	if err != nil {
		return "", nil, fmt.Errorf("inspect worker workspace: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", nil, errors.New("ETL_WORKSPACE_DIR must be a real directory")
	}
	workspace, err := os.MkdirTemp(absRoot, "funnel-publish-")
	if err != nil {
		return "", nil, fmt.Errorf("create private worker workspace: %w", err)
	}
	cleanup := func() {
		if err := os.RemoveAll(workspace); err != nil {
			slogWarnCleanup(workspace, err)
		}
	}
	return workspace, cleanup, nil
}

func slogWarnCleanup(path string, err error) {
	fmt.Fprintf(os.Stderr, "warning: could not remove private workspace %q: %v\n", path, err)
}

func fetchAndHydrateFileWithLimits(ctx context.Context, workspace, commit, sourcePath string, repository githubRepository, github *githubSource, drs drsPuller, limits sourceLimits) (string, error) {
	if limits.maxFileBytes <= 0 {
		return "", errors.New("per-file source size limit must be positive")
	}
	metadataDir := filepath.Join(workspace, "META")
	if err := os.MkdirAll(metadataDir, 0o700); err != nil {
		return "", fmt.Errorf("create metadata staging directory: %w", err)
	}
	destination := filepath.Join(workspace, filepath.FromSlash(sourcePath))
	keepFile := false
	defer func() {
		if !keepFile {
			if err := os.Remove(destination); err != nil && !os.IsNotExist(err) {
				slogWarnCleanup(destination, err)
			}
		}
	}()

	fetchLimit := max(limits.maxFileBytes, maxLFSPointerBytes)
	size, err := github.fetchPinned(ctx, repository, commit, sourcePath, destination, fetchLimit)
	if err != nil {
		return "", err
	}
	pointer, isPointer, err := readLFSPointer(destination, size)
	if err != nil {
		return "", fmt.Errorf("parse Git-DRS pointer %s: %w", sourcePath, err)
	}
	if isPointer {
		if pointer.Size > limits.maxFileBytes {
			return "", fmt.Errorf("Git-DRS payload for %s exceeds the configured per-file size limit", sourcePath)
		}
		pointer.Path = sourcePath
		if err := drs.Pull(ctx, gitdrs.PullOptions{Root: workspace, Files: []gitdrs.File{pointer}, Overwrite: true}); err != nil {
			return "", fmt.Errorf("hydrate Git-DRS metadata %s: %w", sourcePath, err)
		}
	} else if size > limits.maxFileBytes {
		return "", fmt.Errorf("selected metadata file %s exceeds the configured per-file size limit", sourcePath)
	}

	info, err := os.Lstat(destination)
	if err != nil {
		return "", fmt.Errorf("inspect staged metadata %s: %w", sourcePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("staged metadata %s must be a regular file", sourcePath)
	}
	if info.Size() > limits.maxFileBytes {
		return "", fmt.Errorf("staged metadata %s exceeds the configured per-file size limit", sourcePath)
	}
	if err := os.Chmod(destination, 0o400); err != nil {
		return "", fmt.Errorf("make staged metadata %s read-only: %w", sourcePath, err)
	}
	keepFile = true
	return destination, nil
}

func readLFSPointer(path string, size int64) (gitdrs.File, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return gitdrs.File{}, false, err
	}
	defer file.Close()
	prefix := make([]byte, len(lfsPointerVersion))
	n, err := io.ReadFull(file, prefix)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return gitdrs.File{}, false, err
	}
	if string(prefix[:n]) != lfsPointerVersion {
		if bytesHaveLFSVersionPrefix(prefix[:n]) {
			return gitdrs.File{}, false, errors.New("unsupported or malformed Git-LFS pointer header")
		}
		return gitdrs.File{}, false, nil
	}
	if size > maxLFSPointerBytes {
		return gitdrs.File{}, false, errors.New("Git-LFS pointer exceeds the maximum pointer size")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return gitdrs.File{}, false, err
	}
	content, err := io.ReadAll(io.LimitReader(file, maxLFSPointerBytes+1))
	if err != nil {
		return gitdrs.File{}, false, err
	}
	return gitdrs.ParseLFSPointer(content)
}

func bytesHaveLFSVersionPrefix(value []byte) bool {
	return len(value) >= len("version ") && string(value[:len("version ")]) == "version "
}

func validateReceipt(receipt loomingest.Receipt, operationID string) error {
	if receipt.OperationID != operationID {
		return errors.New("Loom receipt operation ID does not match the Funnel task ID")
	}
	if receipt.State != "PUBLISHED" {
		return fmt.Errorf("Loom receipt state %q is not PUBLISHED", receipt.State)
	}
	if receipt.SourceRevision < 0 || receipt.ManifestHash == "" || receipt.ExecutionID == "" {
		return errors.New("Loom receipt is missing source revision, manifest hash, or execution ID")
	}
	return nil
}
