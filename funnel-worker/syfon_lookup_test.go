package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLookupSyfonFilesUsesHashesAndExactProjectScope(t *testing.T) {
	const first = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const second = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method != http.MethodPost || r.URL.Path != "/index/bulk/hashes" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		var request syfonHashRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if len(request.Hashes) != 2 || request.Hashes[0] != "sha256:"+first || request.Hashes[1] != "sha256:"+second {
			t.Fatalf("hash request = %#v", request.Hashes)
		}
		_, _ = fmt.Fprintf(w, `{"results":{"sha256:%s":[{"did":"urn:good","organization":"HTAN_INT","project":"BForePC","name":"file.ndjson","size":42,"hashes":{"sha-256":"%s","md5":"md5-value"},"created_time":"2026-11-20T12:30:00Z","access_methods":[{"access_url":{"url":"https://drs.example/file"}}]},{"did":"urn:other-project","organization":"HTAN_INT","project":"AnotherProject","hashes":{"sha256":"%s"}}]},"records":[{"did":"urn:good","organization":"HTAN_INT","project":"BForePC","hashes":{"sha256":"%s"}},{"did":"urn:controlled","controlled_access":["/organization/HTAN_INT/project/BForePC"],"hashes":{"sha256":"%s"}},{"did":"urn:wrong-hash","organization":"HTAN_INT","project":"BForePC","hashes":{"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}]}`,
			first, first, first, first, second)
	}))
	defer server.Close()

	got, err := lookupSyfonFiles(context.Background(), server.URL, "test-token", "HTAN_INT", "BForePC", []string{"SHA256:" + first, second, first})
	if err != nil {
		t.Fatal(err)
	}
	if requestCount != 1 {
		t.Fatalf("requests = %d, want 1", requestCount)
	}
	if len(got[first]) != 1 {
		t.Fatalf("first hash results = %#v", got[first])
	}
	file := got[first][0]
	if file.ID != "urn:good" || file.Name != "file.ndjson" || file.AccessURL != "https://drs.example/file" ||
		file.MD5 != "md5-value" || file.SHA256 != first || file.Size != 42 ||
		!file.CreatedAt.Equal(time.Date(2026, 11, 20, 12, 30, 0, 0, time.UTC)) {
		t.Fatalf("mapped file = %#v", file)
	}
	if len(got[second]) != 1 || got[second][0].ID != "urn:controlled" {
		t.Fatalf("controlled access results = %#v", got[second])
	}
}

func TestLookupSyfonFilesSkipsEmptyHashRequest(t *testing.T) {
	requested := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requested = true
	}))
	defer server.Close()

	got, err := lookupSyfonFiles(context.Background(), server.URL, "token", "org", "project", []string{"", "sha256:"})
	if err != nil {
		t.Fatal(err)
	}
	if requested || len(got) != 0 {
		t.Fatalf("request=%t result=%#v", requested, got)
	}
}

func TestLookupSyfonFilesDoesNotFollowRedirects(t *testing.T) {
	var redirectedRequest bool
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectedRequest = true
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer source.Close()

	_, err := lookupSyfonFiles(context.Background(), source.URL, "sensitive-token", "org", "project", []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	if err == nil {
		t.Fatal("redirect response was accepted")
	}
	if redirectedRequest {
		t.Fatal("followed redirect with caller request")
	}
}

func TestLookupSyfonFilesChunksLargeHashLists(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body syfonHashRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Hashes) > syfonHashBatchSize {
			t.Fatalf("batch contains %d hashes", len(body.Hashes))
		}
		_, _ = w.Write([]byte(`{"results":{},"records":[]}`))
	}))
	defer server.Close()

	hashes := make([]string, syfonHashBatchSize+1)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("%064x", i)
	}
	_, err := lookupSyfonFiles(context.Background(), server.URL, "", "org", "project", hashes)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}
