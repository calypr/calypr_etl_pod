package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/calypr/git-drs/inventory"
)

func (s *githubSource) listDataPointers(ctx context.Context, repository githubRepository, commit string) ([]inventory.Pointer, error) {
	var revision struct {
		Tree struct{ SHA string } `json:"tree"`
	}
	if err := s.readGitObject(ctx, repository, "commits", strings.ToLower(commit), &revision); err != nil {
		return nil, err
	}
	var tree struct {
		Truncated bool `json:"truncated"`
		Entries   []struct {
			Path string `json:"path"`
			Type string `json:"type"`
			Mode string `json:"mode"`
			SHA  string `json:"sha"`
			Size int64  `json:"size"`
		} `json:"tree"`
	}
	if err := s.readGitObject(ctx, repository, "trees", revision.Tree.SHA, &tree); err != nil {
		return nil, err
	}
	if tree.Truncated {
		return nil, fmt.Errorf("GitHub truncated the data pointer inventory for commit %s", commit)
	}
	sort.Slice(tree.Entries, func(i, j int) bool { return tree.Entries[i].Path < tree.Entries[j].Path })
	var pointers []inventory.Pointer
	for _, entry := range tree.Entries {
		if entry.Type != "blob" || (entry.Mode != "100644" && entry.Mode != "100755") || entry.Size < int64(len(lfsPointerVersion)+len("\noid sha256:\nsize 0")+64) || entry.Size > maxLFSPointerBytes ||
			strings.HasPrefix(entry.Path, "META/") || strings.HasPrefix(entry.Path, "CONFIG/") {
			continue
		}
		blob, err := s.openGitObject(ctx, repository, "blobs", entry.SHA, "application/vnd.github.raw+json")
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(io.LimitReader(blob, maxLFSPointerBytes+1))
		blob.Close()
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != entry.Size {
			return nil, fmt.Errorf("GitHub blob size changed for %s", entry.Path)
		}
		if !bytes.HasPrefix(data, []byte(lfsPointerVersion+"\n")) {
			continue
		}
		pointer, ok, err := parseLFSPointer(data)
		if err != nil {
			return nil, fmt.Errorf("parse data pointer %s: %w", entry.Path, err)
		}
		if ok {
			pointers = append(pointers, inventory.Pointer{Path: entry.Path, SHA256: normalizeSHA256(pointer.OID), Size: pointer.Size})
		}
	}
	return pointers, nil
}

func (s *githubSource) readGitObject(ctx context.Context, repository githubRepository, kind, sha string, out any) error {
	body, err := s.openGitObject(ctx, repository, kind, sha, "application/vnd.github+json")
	if err != nil {
		return err
	}
	defer body.Close()
	if err := json.NewDecoder(io.LimitReader(body, 32<<20)).Decode(out); err != nil {
		return fmt.Errorf("decode GitHub %s response: %w", kind, err)
	}
	return nil
}

func (s *githubSource) openGitObject(ctx context.Context, repository githubRepository, kind, sha, accept string) (io.ReadCloser, error) {
	if validateCommit(sha) != nil {
		return nil, fmt.Errorf("invalid GitHub object ID for %s", kind)
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/git/%s/%s", s.baseURL, repository.Owner, repository.Name, kind, sha)
	if kind == "trees" {
		endpoint += "?recursive=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", accept)
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("User-Agent", githubRequestAgent)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("fetch GitHub %s: HTTP %d", kind, resp.StatusCode)
	}
	return resp.Body, nil
}
