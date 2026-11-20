package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGeckoPointerInventoryRequiresCompleteResponse(t *testing.T) {
	hash := strings.Repeat("a", 64)
	record := `{"path":"z","sha256":"` + hash + `","size":12}`
	for _, response := range []string{"[" + record, "[" + record + "]{}", `[{"path":"z","sha256":"` + hash + `"}]`} {
		if _, err := decodeGeckoDataPointers(strings.NewReader(response)); err == nil {
			t.Fatalf("accepted invalid inventory %q", response)
		}
	}
	decoded, err := decodeGeckoDataPointers(strings.NewReader("[" + record + "," + strings.Replace(record, `"z"`, `"a"`, 1) + "]"))
	if err != nil || len(decoded) != 1 || decoded[hash].Path != "a" || decoded[hash].Size != 12 {
		t.Fatalf("unexpected deduplicated pointers: %v, %v", decoded, err)
	}
}

func TestGeckoPointerInventorySurfacesReadDenial(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	_, err := fetchGeckoDataPointers(context.Background(), server.URL, "token", inputData{Organization: "org", Project: "project", Commit: strings.Repeat("a", 40)})
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected Gecko READ denial, got %v", err)
	}
}

func TestGeckoURLRequiredOnlyForDocumentReferenceLoads(t *testing.T) {
	env := map[string]string{
		"ACCESS_TOKEN": "gen3-token", "GITHUB_TOKEN": "github-token",
		"DRS_URL": "http://syfon", "LOOM_URL": "http://loom",
		"ETL_OPERATION_ID": "task-1", "ETL_WORKSPACE_DIR": "/workspace",
	}
	setInput := func(file string) {
		env["INPUT_DATA"] = `{"projectId":"org-project","organization":"org","project":"project","repository":"https://github.com/org/repo","commit":"` + strings.Repeat("a", 40) + `","files":["` + file + `"]}`
	}
	getenv := func(key string) string { return env[key] }
	setInput("META/Patient.ndjson")
	if _, _, err := loadConfiguration(getenv); err != nil {
		t.Fatalf("ordinary load requires Gecko: %v", err)
	}
	setInput(documentReferencePath)
	if _, _, err := loadConfiguration(getenv); err == nil || !strings.Contains(err.Error(), "GECKO_URL") {
		t.Fatalf("expected missing Gecko URL error, got %v", err)
	}
	env["GECKO_URL"] = "http://gecko-service/gecko"
	config, _, err := loadConfiguration(getenv)
	if err != nil || config.geckoURL != env["GECKO_URL"] {
		t.Fatalf("expected configured Gecko URL, got %q and %v", config.geckoURL, err)
	}
}
