package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/calypr/git-drs/inventory"
)

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
	var pointers []inventory.Pointer
	for _, entry := range entries {
		if entry.GetType() != "blob" || (entry.GetMode() != "100644" && entry.GetMode() != "100755") || entry.GetSize() < len(lfsPointerVersion)+len("\noid sha256:\nsize 0")+64 || int64(entry.GetSize()) > maxLFSPointerBytes ||
			strings.HasPrefix(entry.GetPath(), "META/") || strings.HasPrefix(entry.GetPath(), "CONFIG/") {
			continue
		}
		if validateCommit(entry.GetSHA()) != nil {
			return nil, fmt.Errorf("invalid GitHub blob object ID for %s", entry.GetPath())
		}
		endpoint := fmt.Sprintf("repos/%s/%s/git/blobs/%s", repository.Owner, repository.Name, entry.GetSHA())
		blob, err := s.openRaw(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(io.LimitReader(blob.Body, maxLFSPointerBytes+1))
		blob.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(data) != entry.GetSize() {
			return nil, fmt.Errorf("GitHub blob size changed for %s", entry.GetPath())
		}
		if !bytes.HasPrefix(data, []byte(lfsPointerVersion+"\n")) {
			continue
		}
		pointer, ok, err := parseLFSPointer(data)
		if err != nil {
			return nil, fmt.Errorf("parse data pointer %s: %w", entry.GetPath(), err)
		}
		if ok {
			pointers = append(pointers, inventory.Pointer{Path: entry.GetPath(), SHA256: normalizeSHA256(pointer.OID), Size: pointer.Size})
		}
	}
	return pointers, nil
}
