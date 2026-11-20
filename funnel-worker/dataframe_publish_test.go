package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseDataframePublishInput(t *testing.T) {
	input, err := parseDataframePublishInput(`{"organization":"HTAN_INT","project":"BForePC","explorerId":"dataframe-default","receiptId":"receipt_abc-123"}`)
	if err != nil {
		t.Fatal(err)
	}
	if input.Organization != "HTAN_INT" || input.Project != "BForePC" || input.ExplorerID != "dataframe-default" || input.ReceiptID != "receipt_abc-123" {
		t.Fatalf("parsed publication input = %#v", input)
	}
	for _, raw := range []string{
		`{"organization":"HTAN_INT/other","project":"BForePC","explorerId":"default","receiptId":"receipt_abc"}`,
		`{"organization":"HTAN_INT","project":"BForePC","explorerId":"../other","receiptId":"receipt_abc"}`,
		`{"organization":"HTAN_INT","project":"BForePC","explorerId":"default","receiptId":""}`,
		`{"organization":"HTAN_INT","project":"BForePC","explorerId":"default","receiptId":"receipt_abc","extra":true}`,
		`{"organization":"HTAN_INT","project":"BForePC","explorerId":"default","receiptId":"receipt_abc"} {}`,
	} {
		if _, err := parseDataframePublishInput(raw); err == nil {
			t.Errorf("parseDataframePublishInput(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestPublishDataframeUsesLoomAuthorizationAndReturnsExactReceipt(t *testing.T) {
	const token = "short-lived-access-token"
	const response = `{"apiVersion":"loom.calypr.org/explorer-authoring/v2","kind":"ExplorerBuilderPublication","receiptId":"receipt_abc-123","revisionId":"authoring_abc-123","state":"ACTIVE","outputs":[{"outputId":"Specimen","state":"READY","materializationId":"materialization-1"}],"futureField":{"preserved":true}}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", request.Method)
		}
		if request.URL.EscapedPath() != "/loom/api/v1/projects/HTAN_INT%252FBForePC/explorers/dataframe-default/authoring/v2/publish" {
			t.Errorf("path = %s", request.URL.EscapedPath())
		}
		if got := request.URL.Query().Get("auth_resource_path"); got != "/programs/HTAN_INT/projects/BForePC" {
			t.Errorf("auth resource path = %q", got)
		}
		if got := request.Header.Get("Authorization"); got != "bearer "+token {
			t.Errorf("authorization = %q", got)
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if body["receiptId"] != "receipt_abc-123" {
			t.Errorf("request body = %#v", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, response)
	}))
	defer server.Close()

	var output strings.Builder
	err := runDataframePublishFromEnvironment(func(key string) string {
		return map[string]string{
			"ACCESS_TOKEN": token,
			"LOOM_URL":     server.URL + "/loom",
			"INPUT_DATA":   `{"organization":"HTAN_INT","project":"BForePC","explorerId":"dataframe-default","receiptId":"receipt_abc-123"}`,
		}[key]
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if want := "LOOM_PUBLICATION_RESULT {\"apiVersion\":\"loom.calypr.org/explorer-authoring/v2\",\"kind\":\"ExplorerBuilderPublication\",\"receiptId\":\"receipt_abc-123\",\"revisionId\":\"authoring_abc-123\",\"state\":\"ACTIVE\",\"outputs\":[{\"outputId\":\"Specimen\",\"state\":\"READY\",\"materializationId\":\"materialization-1\"}],\"futureField\":{\"preserved\":true}}\n"; output.String() != want {
		t.Fatalf("stdout = %s, want %s", output.String(), want)
	}
}

func TestPublishDataframeRejectsRedirectWithoutFollowingIt(t *testing.T) {
	var destinationRequests int
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		destinationRequests++
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	var output strings.Builder
	err := runDataframePublishFromEnvironment(func(key string) string {
		return map[string]string{
			"ACCESS_TOKEN": "short-lived-token",
			"LOOM_URL":     redirect.URL,
			"INPUT_DATA":   `{"organization":"HTAN_INT","project":"BForePC","explorerId":"default","receiptId":"receipt_1"}`,
		}[key]
	}, &output)
	if err == nil {
		t.Fatal("publication unexpectedly succeeded after redirect")
	}
	if destinationRequests != 0 {
		t.Fatalf("followed redirect to another host %d times", destinationRequests)
	}
	if output.Len() != 0 {
		t.Fatalf("failure emitted success output %q", output.String())
	}
}
