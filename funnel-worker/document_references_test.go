package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	loomingest "github.com/calypr/loom/client/ingest"
)

func TestRunJobReconcilesOnlySelectedDocumentReference(t *testing.T) {
	commit := strings.Repeat("a", 40)
	authoredHash := strings.Repeat("1", 64)
	generatedHash := strings.Repeat("2", 64)
	missingHash := strings.Repeat("3", 64)
	mux := http.NewServeMux()
	mux.HandleFunc("/gecko/git/projects/org/project/pointer-inventory", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("commit") != commit {
			t.Error("Gecko inventory request must use the pinned commit")
		}
		if r.Header.Get("Authorization") != "Bearer gen3-token" {
			t.Error("Gecko inventory must use the Gen3 token")
		}
		fmt.Fprintf(w, `[{"path":"data/one","sha256":%q,"size":7},{"path":"data/two","sha256":%q,"size":7},{"path":"data/missing","sha256":%q,"size":7}]`, authoredHash, generatedHash, missingHash)
	})
	mux.HandleFunc("/api/v3/repos/calypr/meta/contents/META/DocumentReference.ndjson", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("ref") != commit {
			t.Error("DocumentReference request must use the pinned commit")
		}
		fmt.Fprintf(w, `{"resourceType":"DocumentReference","id":"authored","description":"retain me","identifier":[{"system":%q,"value":%q}]}`+"\n", fileSHA256System, authoredHash)
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
	config := jobConfig{workspaceDir: t.TempDir(), operationID: "task", drsURL: server.URL, geckoURL: server.URL + "/gecko", accessToken: "gen3-token"}
	input := inputData{ProjectID: "org-project", Organization: "org", Project: "project", Repository: "https://github.com/calypr/meta", Commit: commit, Files: []string{documentReferencePath}}
	result, err := runJob(context.Background(), config, input, workerDependencies{github: source, drs: &recordingDRS{}, publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	if len(publisher.fileContents) != 1 {
		t.Fatalf("uploaded unselected files: %v", publisher.fileContents)
	}
	if len(result.Warnings) != 1 || result.Warnings[0] != (sourceLoadWarning{Code: "MISSING_SYFON_MATCHES", Count: 1}) {
		t.Fatalf("missing Syfon result warning: %+v", result.Warnings)
	}
	data := publisher.fileContents[documentReferencePath]
	if strings.Contains(data, `"subject"`) {
		t.Fatalf("generated subject without a selected study: %s", data)
	}
	if strings.Count(data, "\n") != 2 {
		t.Fatalf("expected two NDJSON records, got %s", data)
	}
	for _, expected := range []string{"authored-drs", "retain me", "generated-drs", `"size":"7"`} {
		if !strings.Contains(data, expected) {
			t.Errorf("published DocumentReference missing %q: %s", expected, data)
		}
	}
}

func TestReconcileDocumentReferencePreservesUnchangedRowsAndOriginalOnFailure(t *testing.T) {
	line := `{ "resourceType": "DocumentReference", "id": "authored" }` + "\n"
	path := filepath.Join(t.TempDir(), "DocumentReference.ndjson")
	if err := os.WriteFile(path, []byte(line), 0o400); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{int64(len(line)), int64(len(line) - 1)} {
		_, err := reconcileDocumentReferenceFile(context.Background(), path, "https://syfon.example", "project", nil, nil, nil, limit, nil)
		if (err != nil) != (limit < int64(len(line))) {
			t.Fatalf("limit %d returned %v", limit, err)
		}
		saved, err := os.ReadFile(path)
		if err != nil || string(saved) != line {
			t.Fatalf("authored record changed: %q, %v", saved, err)
		}
	}
}

func TestSetDocumentReferenceDRSIDKeepsOtherOfficialIdentifiers(t *testing.T) {
	const system = "https://syfon.example/project"
	original := json.RawMessage(`[{"system":"https://example.org/accession","use":"official","value":"did-1","period":{"start":"2020"}},{"system":"https://syfon.example/project","use":"official","value":"stale"}]`)
	row := ndjsonRecord{"identifier": original}
	changed, err := setDocumentReferenceDRSID(row, system, "did-1")
	if err != nil || !changed {
		t.Fatalf("expected DRS correction, got changed=%v err=%v", changed, err)
	}
	var identifiers []json.RawMessage
	if err := json.Unmarshal(row["identifier"], &identifiers); err != nil {
		t.Fatal(err)
	}
	var oldIdentifiers []json.RawMessage
	if err := json.Unmarshal(original, &oldIdentifiers); err != nil {
		t.Fatal(err)
	}
	if len(identifiers) != 2 || !strings.Contains(string(identifiers[0]), system) || !strings.Contains(string(identifiers[0]), "did-1") ||
		string(identifiers[1]) != string(oldIdentifiers[0]) {
		t.Fatalf("identifier correction lost authored values: %s", row["identifier"])
	}
	changed, err = setDocumentReferenceDRSID(row, system, "did-1")
	if err != nil || changed {
		t.Fatalf("expected idempotent DRS correction, got changed=%v err=%v", changed, err)
	}
}
