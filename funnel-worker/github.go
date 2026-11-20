package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type githubSource struct {
	baseURL string
	token   string
	client  *http.Client
}

func newGitHubSource(token string, client *http.Client) (*githubSource, error) {
	return newGitHubSourceAt(githubAPIBaseURL, token, client)
}

func newGitHubSourceAt(baseURL, token string, client *http.Client) (*githubSource, error) {
	baseURL, err := validateServiceURL(baseURL, "GitHub API URL")
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &githubSource{baseURL: baseURL, token: token, client: &copyClient}, nil
}

func (s *githubSource) fetchPinned(ctx context.Context, repository githubRepository, commit, path, destination string, limit int64) (int64, error) {
	if limit < 0 || limit > maxFileBytes {
		return 0, errors.New("invalid GitHub file size limit")
	}
	name := strings.TrimPrefix(path, "META/")
	endpoint := fmt.Sprintf("%s/repos/%s/%s/contents/META/%s?ref=%s", strings.TrimRight(s.baseURL, "/"), url.PathEscape(repository.Owner), url.PathEscape(repository.Name), url.PathEscape(name), url.QueryEscape(strings.ToLower(commit)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("create pinned GitHub request for %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "application/vnd.github.raw+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("User-Agent", githubRequestAgent)
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch pinned GitHub file %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("fetch pinned GitHub file %s: HTTP %d", path, resp.StatusCode)
	}
	if resp.ContentLength > limit {
		return 0, fmt.Errorf("GitHub file %s exceeds the configured size limit", path)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return 0, fmt.Errorf("create staging directory for %s: %w", path, err)
	}
	staged, err := os.CreateTemp(filepath.Dir(destination), ".download-")
	if err != nil {
		return 0, fmt.Errorf("stage GitHub file %s: %w", path, err)
	}
	stagedPath := staged.Name()
	removeStaged := true
	defer func() {
		if removeStaged {
			_ = os.Remove(stagedPath)
		}
	}()
	count, copyErr := io.Copy(staged, io.LimitReader(resp.Body, limit+1))
	closeErr := staged.Close()
	if copyErr != nil {
		return 0, fmt.Errorf("read pinned GitHub file %s: %w", path, copyErr)
	}
	if count > limit {
		return 0, fmt.Errorf("GitHub file %s exceeds the configured size limit", path)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("finish staging GitHub file %s: %w", path, closeErr)
	}
	if err := os.Rename(stagedPath, destination); err != nil {
		return 0, fmt.Errorf("install staged GitHub file %s: %w", path, err)
	}
	removeStaged = false
	return count, nil
}
