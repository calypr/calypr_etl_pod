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

	github "github.com/google/go-github/v87/github"
)

type githubSource struct {
	client *github.Client
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
	options := []github.ClientOptionsFunc{
		github.WithHTTPClient(&copyClient),
		github.WithAuthToken(token),
		github.WithUserAgent(githubRequestAgent),
	}
	if baseURL != githubAPIBaseURL {
		options = append(options, github.WithEnterpriseURLs(baseURL, baseURL))
	}
	api, err := github.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("configure GitHub API client: %w", err)
	}
	return &githubSource{client: api}, nil
}

func (s *githubSource) fetchPinned(ctx context.Context, repository githubRepository, commit, path, destination string, limit int64) (int64, error) {
	if limit < 0 || limit > maxFileBytes {
		return 0, errors.New("invalid GitHub file size limit")
	}
	name := strings.TrimPrefix(path, "META/")
	endpoint := fmt.Sprintf("repos/%s/%s/contents/META/%s?ref=%s", url.PathEscape(repository.Owner), url.PathEscape(repository.Name), url.PathEscape(name), url.QueryEscape(strings.ToLower(commit)))
	resp, err := s.openRaw(ctx, endpoint)
	if err != nil {
		return 0, fmt.Errorf("fetch pinned GitHub file %s: %w", path, err)
	}
	defer resp.Body.Close()
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

func (s *githubSource) openRaw(ctx context.Context, endpoint string) (*http.Response, error) {
	req, err := s.client.NewRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.raw+json")
	resp, err := s.client.BareDo(req)
	if err != nil {
		return nil, githubResponseError(resp, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.Response, nil
}

func githubResponseError(resp *github.Response, err error) error {
	if resp != nil && resp.Response != nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return err
}
