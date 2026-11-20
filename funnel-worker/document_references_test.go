package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	loomingest "github.com/calypr/loom/client/ingest"
)

func TestRunJobReconcilesOnlySelectedDocumentReference(t *testing.T) {
	commit := strings.Repeat("a", 40)
	tree := strings.Repeat("b", 40)
	firstBlob := strings.Repeat("c", 40)
	secondBlob := strings.Repeat("d", 40)
	authoredHash := strings.Repeat("1", 64)
	generatedHash := strings.Repeat("2", 64)
	pointer := func(hash string) string {
		return fmt.Sprintf("%s\noid sha256:%s\nsize 7\n", lfsPointerVersion, hash)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/calypr/meta/git/commits/"+commit, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"tree":{"sha":%q}}`, tree)
	})
	mux.HandleFunc("/repos/calypr/meta/git/trees/"+tree, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") != "1" {
			t.Error("expected recursive inventory")
		}
		fmt.Fprintf(w, `{"tree":[{"path":"data/one","mode":"100644","type":"blob","sha":%q,"size":%d},{"path":"data/two","mode":"100644","type":"blob","sha":%q,"size":%d}]}`, firstBlob, len(pointer(authoredHash)), secondBlob, len(pointer(generatedHash)))
	})
	mux.HandleFunc("/repos/calypr/meta/git/blobs/"+firstBlob, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, pointer(authoredHash)) })
	mux.HandleFunc("/repos/calypr/meta/git/blobs/"+secondBlob, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, pointer(generatedHash)) })
	mux.HandleFunc("/repos/calypr/meta/contents/META/DocumentReference.ndjson", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("ref") != commit {
			t.Error("DocumentReference request must use the pinned commit")
		}
		fmt.Fprintf(w, `{"resourceType":"DocumentReference","id":"authored","description":"retain me","identifier":[{"system":%q,"value":%q}]}`+"\n", fileSHA256System, authoredHash)
	})
	mux.HandleFunc("/repos/calypr/meta/contents/META/ResearchStudy.ndjson", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("ref") != commit {
			t.Error("ResearchStudy request must use the pinned commit")
		}
		fmt.Fprint(w, `{"resourceType":"ResearchStudy","id":"study"}`+"\n")
	})
	mux.HandleFunc("/index/bulk/hashes", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer gen3-token" {
			t.Error("Syfon lookup must use the Gen3 token")
		}
		fmt.Fprintf(w, `{"records":[{"organization":"org","project":"project","did":"authored-drs","hashes":{"sha256":%q}},{"organization":"org","project":"project","did":"generated-drs","name":"two","hashes":{"sha256":%q}}]}`, authoredHash, generatedHash)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	source, err := newGitHubSourceAt(server.URL, "github-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{receipt: loomingest.Receipt{OperationID: "task", State: "PUBLISHED", ManifestHash: "manifest", ExecutionID: "execution"}}
	config := jobConfig{workspaceDir: t.TempDir(), operationID: "task", drsURL: server.URL, accessToken: "gen3-token"}
	input := inputData{ProjectID: "org-project", Organization: "org", Project: "project", Repository: "https://github.com/calypr/meta", Commit: commit, Files: []string{documentReferencePath}}
	if _, err := runJob(context.Background(), config, input, workerDependencies{github: source, drs: &recordingDRS{}, publisher: publisher}); err != nil {
		t.Fatal(err)
	}
	if len(publisher.fileContents) != 1 {
		t.Fatalf("uploaded unselected files: %v", publisher.fileContents)
	}
	data := publisher.fileContents[documentReferencePath]
	if strings.Count(data, "\n") != 2 {
		t.Fatalf("expected two NDJSON records, got %s", data)
	}
	for _, expected := range []string{"authored-drs", "retain me", "generated-drs", "ResearchStudy/study", `"size":"7"`} {
		if !strings.Contains(data, expected) {
			t.Errorf("published DocumentReference missing %q: %s", expected, data)
		}
	}
}
