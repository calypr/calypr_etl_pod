package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/git-drs/inventory"
)

func fetchGeckoDataPointers(ctx context.Context, baseURL, token string, input inputData) (map[string]inventory.Pointer, error) {
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint.Host == "" {
		return nil, errors.New("GECKO_URL is required for DocumentReference reconciliation")
	}
	endpoint = endpoint.JoinPath("git", "projects", input.Organization, input.Project, "pointer-inventory")
	query := endpoint.Query()
	query.Set("commit", strings.ToLower(input.Commit))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build Gecko pointer inventory request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get Gecko pointer inventory: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Gecko pointer inventory returned HTTP %d", response.StatusCode)
	}
	pointers, err := decodeGeckoDataPointers(response.Body)
	if err != nil {
		return nil, fmt.Errorf("decode Gecko pointer inventory: %w", err)
	}
	return pointers, nil
}

func decodeGeckoDataPointers(body io.Reader) (map[string]inventory.Pointer, error) {
	decoder := json.NewDecoder(body)
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start != json.Delim('[') {
		return nil, errors.New("pointer inventory must be a JSON array")
	}
	pointers := make(map[string]inventory.Pointer)
	for decoder.More() {
		var row struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
			Size   *int64 `json:"size"`
		}
		if err := decoder.Decode(&row); err != nil {
			return nil, err
		}
		if _, err := hex.DecodeString(row.SHA256); err != nil || len(row.SHA256) != 64 ||
			row.Path == "" || row.Size == nil || *row.Size < 0 {
			return nil, errors.New("invalid pointer record")
		}
		hash := strings.ToLower(row.SHA256)
		if previous, exists := pointers[hash]; !exists || row.Path < previous.Path {
			pointers[hash] = inventory.Pointer{Path: row.Path, SHA256: hash, Size: *row.Size}
		}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
		return nil, errors.New("incomplete pointer inventory")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after pointer inventory")
	}
	return pointers, nil
}
