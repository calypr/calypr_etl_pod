package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	gitdrs "github.com/calypr/git-drs/client"
	"github.com/calypr/git-drs/inventory"
)

const githubBlobReadConcurrency = 8

type pointerBlob struct {
	path string
	sha  string
	size int
}

type pointerBlobResult struct {
	oid  string
	size int64
	ok   bool
	err  error
}

func (s *githubSource) listDataPointers(ctx context.Context, repository githubRepository, commit string) ([]inventory.Pointer, error) {
	revision, resp, err := s.client.Git.GetCommit(ctx, repository.Owner, repository.Name, strings.ToLower(commit))
	if err != nil {
		return nil, fmt.Errorf("fetch GitHub commit: %w", githubResponseError(resp, err))
	}
	treeSHA := revision.GetTree().GetSHA()
	if validateCommit(treeSHA) != nil {
		return nil, fmt.Errorf("invalid GitHub tree object ID")
	}
	tree, resp, err := s.client.Git.GetTree(ctx, repository.Owner, repository.Name, treeSHA, true)
	if err != nil {
		return nil, fmt.Errorf("fetch GitHub tree: %w", githubResponseError(resp, err))
	}
	if tree.GetTruncated() {
		return nil, fmt.Errorf("GitHub truncated the data pointer inventory for commit %s", commit)
	}
	entries := tree.Entries
	sort.Slice(entries, func(i, j int) bool { return entries[i].GetPath() < entries[j].GetPath() })
	blobs := make([]pointerBlob, 0)
	blobIndexes := make(map[string]int)
	for _, entry := range entries {
		if entry.GetType() != "blob" || (entry.GetMode() != "100644" && entry.GetMode() != "100755") || entry.GetSize() < len(lfsPointerVersion)+len("\noid sha256:\nsize 0")+64 || int64(entry.GetSize()) > maxLFSPointerBytes ||
			strings.HasPrefix(entry.GetPath(), "META/") || strings.HasPrefix(entry.GetPath(), "CONFIG/") {
			continue
		}
		if validateCommit(entry.GetSHA()) != nil {
			return nil, fmt.Errorf("invalid GitHub blob object ID for %s", entry.GetPath())
		}
		if _, exists := blobIndexes[entry.GetSHA()]; !exists {
			blobIndexes[entry.GetSHA()] = len(blobs)
			blobs = append(blobs, pointerBlob{path: entry.GetPath(), sha: entry.GetSHA(), size: entry.GetSize()})
		}
	}

	results := make([]pointerBlobResult, len(blobs))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	workerCount := min(githubBlobReadConcurrency, len(blobs))
	var workers sync.WaitGroup
	var firstError error
	var firstErrorPath string
	var errorOnce sync.Once
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					result := s.readPointerBlob(workCtx, repository, blobs[index])
					results[index] = result
					if result.err != nil {
						errorOnce.Do(func() {
							firstError = result.err
							firstErrorPath = blobs[index].path
						})
						cancel()
						return
					}
				}
			}
		}()
	}

sendJobs:
	for index := range blobs {
		select {
		case <-workCtx.Done():
			break sendJobs
		case jobs <- index:
		}
	}
	close(jobs)
	workers.Wait()
	if firstError != nil {
		return nil, fmt.Errorf("inspect data pointer %s: %w", firstErrorPath, firstError)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var pointers []inventory.Pointer
	for _, entry := range entries {
		if entry.GetType() != "blob" || (entry.GetMode() != "100644" && entry.GetMode() != "100755") || entry.GetSize() < len(lfsPointerVersion)+len("\noid sha256:\nsize 0")+64 || int64(entry.GetSize()) > maxLFSPointerBytes ||
			strings.HasPrefix(entry.GetPath(), "META/") || strings.HasPrefix(entry.GetPath(), "CONFIG/") {
			continue
		}
		result := results[blobIndexes[entry.GetSHA()]]
		if result.ok {
			pointers = append(pointers, inventory.Pointer{Path: entry.GetPath(), SHA256: result.oid, Size: result.size})
		}
	}
	return pointers, nil
}

func (s *githubSource) readPointerBlob(ctx context.Context, repository githubRepository, blob pointerBlob) pointerBlobResult {
	endpoint := fmt.Sprintf("repos/%s/%s/git/blobs/%s", repository.Owner, repository.Name, blob.sha)
	response, err := s.openRaw(ctx, endpoint)
	if err != nil {
		return pointerBlobResult{err: err}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxLFSPointerBytes+1))
	if err != nil {
		return pointerBlobResult{err: err}
	}
	if len(data) != blob.size {
		return pointerBlobResult{err: fmt.Errorf("GitHub blob size changed from %d to %d bytes", blob.size, len(data))}
	}
	if !bytes.HasPrefix(data, []byte(lfsPointerVersion+"\n")) {
		return pointerBlobResult{}
	}
	pointer, ok, err := gitdrs.ParseLFSPointer(data)
	if err != nil {
		return pointerBlobResult{err: err}
	}
	return pointerBlobResult{oid: normalizeSHA256(pointer.OID), size: pointer.Size, ok: ok}
}
