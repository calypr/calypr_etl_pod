package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/calypr/git-drs/inventory"
	"github.com/google/uuid"
)

func TestReconcileDocumentReferencesPreservesAuthoredRowsAndGeneratesMissingRows(t *testing.T) {
	dir := t.TempDir()
	endpoint := "https://calypr.example"
	project := "Example"
	authoredHash := strings.Repeat("a", 64)
	generatedHash := strings.Repeat("b", 64)
	missingHash := strings.Repeat("c", 64)
	documents := filepath.Join(dir, "DocumentReference.ndjson")
	authored := `{"resourceType":"DocumentReference","id":"authored","status":"current","description":"retain this","subject":{"reference":"Patient/patient"},"identifier":[{"use":"official","system":"old","value":"legacy"},{"system":"` + fileSHA256System + `","value":"` + authoredHash + `"}],"content":[{"attachment":{"title":"Authored"}}]}`
	if err := os.WriteFile(documents, []byte(authored+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	study := `{"resourceType":"ResearchStudy","id":"authored-study","status":"active","title":"Retain title"}`
	if err := os.WriteFile(filepath.Join(dir, "ResearchStudy.ndjson"), []byte(study+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pointers := map[string]inventory.Pointer{generatedHash: {Path: "data/generated.txt", Size: 123}}
	objects := map[string][]syfonFile{
		authoredHash:  {{ID: "authored-drs"}},
		generatedHash: {{ID: "generated-drs", MD5: "checksum", AccessURL: "drs://calypr.example/generated-drs", CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}},
	}
	hashes := []string{authoredHash, generatedHash, missingHash}
	for range 2 {
		if err := reconcileDocumentReferenceFiles(context.Background(), dir, endpoint, project, hashes, pointers, nil, objects); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := readNDJSONRecords(documents)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected one authored and one generated row, got %d", len(rows))
	}
	if string(rows[0]["description"]) != `"retain this"` || string(rows[0]["subject"]) != `{"reference":"Patient/patient"}` {
		t.Fatalf("authored content changed: %v", rows[0])
	}
	for i, id := range []string{"authored-drs", "generated-drs"} {
		var identifiers []map[string]string
		if err := json.Unmarshal(rows[i]["identifier"], &identifiers); err != nil {
			t.Fatal(err)
		}
		if len(identifiers) != 2 || identifiers[0]["value"] != id || identifiers[0]["system"] != endpoint+"/"+project || identifiers[1]["system"] != fileSHA256System {
			t.Fatalf("row %d identifiers = %#v", i, identifiers)
		}
	}
	var content []struct {
		Attachment map[string]any
	}
	if err := json.Unmarshal(rows[1]["content"], &content); err != nil {
		t.Fatal(err)
	}
	if len(content) != 1 || content[0].Attachment["size"] != "123" || content[0].Attachment["title"] != "generated.txt" {
		t.Fatalf("generated attachment = %#v", content)
	}
	namespace := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(endpoint))
	wantID, _ := json.Marshal(uuid.NewSHA1(namespace, []byte(project+"/sha256:"+generatedHash)).String())
	if !reflect.DeepEqual([]byte(rows[1]["id"]), wantID) || string(rows[1]["subject"]) != `{"reference":"ResearchStudy/authored-study"}` {
		t.Fatalf("generated identity or relationship changed: %v", rows[1])
	}
	if saved, err := os.ReadFile(filepath.Join(dir, "ResearchStudy.ndjson")); err != nil || string(saved) != study+"\n" {
		t.Fatalf("authored study changed: %s, %v", saved, err)
	}
}

func TestReconcileDocumentReferencesRejectsAmbiguousNewRecordsBeforeWriting(t *testing.T) {
	dir := t.TempDir()
	hash := strings.Repeat("a", 64)
	err := reconcileDocumentReferenceFiles(context.Background(), dir, "https://calypr.example", "Example",
		[]string{hash}, nil, nil, map[string][]syfonFile{hash: {{ID: "first"}, {ID: "second"}}})
	if err == nil || !strings.Contains(err.Error(), "2 records") {
		t.Fatalf("expected ambiguous record error, got %v", err)
	}
	for _, resource := range []string{"DocumentReference", "ResearchStudy"} {
		if _, err := os.Stat(filepath.Join(dir, resource+".ndjson")); !os.IsNotExist(err) {
			t.Fatalf("reconciliation wrote %s before rejecting ambiguity: %v", resource, err)
		}
	}
}
