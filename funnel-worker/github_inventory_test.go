package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestListDataPointersReadsUniqueBlobsWithBoundedConcurrency(t *testing.T) {
	commit := strings.Repeat("a", 40)
	treeSHA := strings.Repeat("b", 40)
	blobSHAs := make([]string, 12)
	for index := range blobSHAs {
		blobSHAs[index] = fmt.Sprintf("%040x", index+1)
	}
	var active atomic.Int32
	var maximum atomic.Int32
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v3/repos/calypr/meta/git/commits/" + commit:
			fmt.Fprintf(writer, `{"tree":{"sha":%q}}`, treeSHA)
		case "/api/v3/repos/calypr/meta/git/trees/" + treeSHA:
			fmt.Fprint(writer, `{"tree":[`)
			for index, sha := range blobSHAs {
				if index > 0 {
					fmt.Fprint(writer, ",")
				}
				fmt.Fprintf(writer, `{"path":"data/%02d","mode":"100644","type":"blob","sha":%q,"size":%d}`, index, sha, len(pointerPayload(index)))
			}
			fmt.Fprintf(writer, `,{"path":"data/duplicate","mode":"100644","type":"blob","sha":%q,"size":%d}]}`, blobSHAs[0], len(pointerPayload(0)))
		default:
			prefix := "/api/v3/repos/calypr/meta/git/blobs/"
			if !strings.HasPrefix(request.URL.Path, prefix) {
				http.NotFound(writer, request)
				return
			}
			sha := strings.TrimPrefix(request.URL.Path, prefix)
			index := -1
			for candidate, value := range blobSHAs {
				if sha == value {
					index = candidate
					break
				}
			}
			if index < 0 {
				http.NotFound(writer, request)
				return
			}
			requests.Add(1)
			current := active.Add(1)
			for observed := maximum.Load(); current > observed; observed = maximum.Load() {
				if maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			fmt.Fprint(writer, pointerPayload(index))
			active.Add(-1)
		}
	}))
	defer server.Close()

	source, err := newGitHubSourceAt(server.URL+"/api/v3", "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	repository, err := parseGitHubRepository("https://github.com/calypr/meta")
	if err != nil {
		t.Fatal(err)
	}
	pointers, err := source.listDataPointers(context.Background(), repository, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := requests.Load(), int32(len(blobSHAs)); got != want {
		t.Fatalf("blob requests = %d, want %d unique blobs", got, want)
	}
	if got := maximum.Load(); got < 2 || got > githubBlobReadConcurrency {
		t.Fatalf("maximum concurrent reads = %d, want between 2 and %d", got, githubBlobReadConcurrency)
	}
	if len(pointers) != len(blobSHAs)+1 {
		t.Fatalf("pointer count = %d, want %d including duplicate path", len(pointers), len(blobSHAs)+1)
	}
	for index, pointer := range pointers {
		if index > 0 && pointers[index-1].Path > pointer.Path {
			t.Fatalf("pointer paths are not sorted: %q before %q", pointers[index-1].Path, pointer.Path)
		}
	}
}

func TestListDataPointersStopsSchedulingAfterBlobFailure(t *testing.T) {
	commit := strings.Repeat("a", 40)
	treeSHA := strings.Repeat("b", 40)
	const blobCount = 24
	blobSHAs := make([]string, blobCount)
	for index := range blobSHAs {
		blobSHAs[index] = fmt.Sprintf("%040x", index+1)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v3/repos/calypr/meta/git/commits/" + commit:
			fmt.Fprintf(writer, `{"tree":{"sha":%q}}`, treeSHA)
		case "/api/v3/repos/calypr/meta/git/trees/" + treeSHA:
			fmt.Fprint(writer, `{"tree":[`)
			for index, sha := range blobSHAs {
				if index > 0 {
					fmt.Fprint(writer, ",")
				}
				fmt.Fprintf(writer, `{"path":"data/%02d","mode":"100644","type":"blob","sha":%q,"size":%d}`, index, sha, len(pointerPayload(index)))
			}
			fmt.Fprint(writer, "]}")
		default:
			requests.Add(1)
			if strings.HasSuffix(request.URL.Path, blobSHAs[0]) {
				http.Error(writer, "missing blob", http.StatusForbidden)
				return
			}
			time.Sleep(20 * time.Millisecond)
			index := 0
			for candidate, sha := range blobSHAs {
				if strings.HasSuffix(request.URL.Path, sha) {
					index = candidate
					break
				}
			}
			fmt.Fprint(writer, pointerPayload(index))
		}
	}))
	defer server.Close()

	source, err := newGitHubSourceAt(server.URL+"/api/v3", "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	repository, err := parseGitHubRepository("https://github.com/calypr/meta")
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.listDataPointers(context.Background(), repository, commit)
	if err == nil || !strings.Contains(err.Error(), "data/00") {
		t.Fatalf("expected a path-specific blob error, got %v", err)
	}
	if got := requests.Load(); got >= blobCount {
		t.Fatalf("started %d blob reads after a failure with %d candidates", got, blobCount)
	}
}

func pointerPayload(index int) string {
	return fmt.Sprintf("%s\noid sha256:%064x\nsize %d\n", lfsPointerVersion, index+100, index+1)
}
