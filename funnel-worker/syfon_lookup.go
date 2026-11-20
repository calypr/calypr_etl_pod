package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bytedance/sonic"
)

const syfonHashBatchSize = 500

type syfonFile struct {
	ID               string
	Name             string
	AccessURL        string
	MD5              string
	CreatedAt        time.Time
	Size             int64
	SHA256           string
	ControlledAccess []string
}

type syfonHashRequest struct {
	Hashes []string `json:"hashes"`
}

type syfonHashResponse struct {
	Results map[string][]syfonHashRecord `json:"results"`
	Records []syfonHashRecord            `json:"records"`
}

type syfonHashRecord struct {
	AccessMethods    []syfonAccessMethod `json:"access_methods"`
	ControlledAccess []string            `json:"controlled_access"`
	CreatedTime      string              `json:"created_time"`
	Did              string              `json:"did"`
	Name             string              `json:"name"`
	FileName         string              `json:"file_name"`
	Hashes           map[string]string   `json:"hashes"`
	Organization     string              `json:"organization"`
	Project          string              `json:"project"`
	Size             int64               `json:"size"`
}

type syfonAccessMethod struct {
	AccessURL *struct {
		URL string `json:"url"`
	} `json:"access_url"`
}

func lookupSyfonFiles(
	ctx context.Context,
	endpoint string,
	token string,
	organization string,
	project string,
	hashes []string,
) (map[string][]syfonFile, error) {
	requestHashes := normalizedRequestedHashes(hashes)
	result := make(map[string][]syfonFile, len(requestHashes))
	requestedSet := make(map[string]struct{}, len(requestHashes))
	for _, hash := range requestHashes {
		result[hash] = []syfonFile{}
		requestedSet[hash] = struct{}{}
	}
	if len(requestHashes) == 0 {
		return result, nil
	}

	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(endpoint), "/"))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return nil, fmt.Errorf("Syfon endpoint is invalid")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/index/bulk/hashes"

	seen := make(map[string]map[string]struct{}, len(requestHashes))
	for hash := range requestedSet {
		seen[hash] = make(map[string]struct{})
	}

	client := &http.Client{
		Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for start := 0; start < len(requestHashes); start += syfonHashBatchSize {
		end := start + syfonHashBatchSize
		if end > len(requestHashes) {
			end = len(requestHashes)
		}
		batch := requestHashes[start:end]
		body, err := sonic.ConfigStd.Marshal(syfonHashRequest{Hashes: prefixedSHA256Hashes(batch)})
		if err != nil {
			return nil, fmt.Errorf("encode Syfon hash lookup: %w", err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create Syfon hash lookup request: %w", err)
		}
		request.Header.Set("Content-Type", "application/json")
		if accessToken := strings.TrimSpace(token); accessToken != "" {
			request.Header.Set("Authorization", "Bearer "+accessToken)
		}

		response, err := client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("query Syfon objects by checksum: %w", err)
		}
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		closeErr := response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read Syfon hash lookup response: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close Syfon hash lookup response: %w", closeErr)
		}
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("Syfon hash lookup returned HTTP %d", response.StatusCode)
		}

		var decoded syfonHashResponse
		if err := sonic.ConfigStd.Unmarshal(payload, &decoded); err != nil {
			return nil, fmt.Errorf("decode Syfon hash lookup response: %w", err)
		}
		var records []syfonHashRecord
		for _, matching := range decoded.Results {
			records = append(records, matching...)
		}
		records = append(records, decoded.Records...)
		for _, record := range records {
			if !syfonRecordMatchesScope(record, organization, project) {
				continue
			}
			file, ok := syfonFileFromRecord(record)
			if !ok {
				continue
			}
			for hash := range fileSHA256Hashes(record.Hashes) {
				if _, requested := requestedSet[hash]; !requested {
					continue
				}
				if _, duplicate := seen[hash][file.ID]; duplicate {
					continue
				}
				seen[hash][file.ID] = struct{}{}
				result[hash] = append(result[hash], file)
			}
		}
	}
	return result, nil
}

func normalizedRequestedHashes(hashes []string) []string {
	seen := make(map[string]struct{}, len(hashes))
	result := make([]string, 0, len(hashes))
	for _, value := range hashes {
		hash := normalizeSHA256(value)
		if hash == "" {
			continue
		}
		if _, exists := seen[hash]; exists {
			continue
		}
		seen[hash] = struct{}{}
		result = append(result, hash)
	}
	return result
}

func prefixedSHA256Hashes(hashes []string) []string {
	result := make([]string, len(hashes))
	for i, hash := range hashes {
		result[i] = "sha256:" + hash
	}
	return result
}

func normalizeSHA256(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(strings.ToLower(value), "sha256:")
	return strings.TrimSpace(value)
}

func fileSHA256Hashes(hashes map[string]string) map[string]struct{} {
	result := make(map[string]struct{})
	for kind, value := range hashes {
		kind = strings.ToLower(strings.TrimSpace(kind))
		if kind != "sha256" && kind != "sha-256" {
			continue
		}
		if hash := normalizeSHA256(value); hash != "" {
			result[hash] = struct{}{}
		}
	}
	return result
}

func syfonRecordMatchesScope(record syfonHashRecord, organization, project string) bool {
	recordOrganization := strings.TrimSpace(record.Organization)
	recordProject := strings.TrimSpace(record.Project)
	if recordOrganization != "" && recordOrganization != organization {
		return false
	}
	if recordProject != "" && recordProject != project {
		return false
	}
	if recordOrganization != "" && recordProject != "" {
		return true
	}
	expected := "/organization/" + strings.TrimSpace(organization) + "/project/" + strings.TrimSpace(project)
	for _, scope := range record.ControlledAccess {
		if strings.TrimSpace(scope) == expected {
			return true
		}
	}
	return strings.TrimSpace(organization) == "" && strings.TrimSpace(project) == ""
}

func syfonFileFromRecord(record syfonHashRecord) (syfonFile, bool) {
	id := strings.TrimSpace(record.Did)
	if id == "" {
		return syfonFile{}, false
	}
	name := strings.TrimSpace(record.Name)
	if name == "" {
		name = strings.TrimSpace(record.FileName)
	}
	file := syfonFile{
		ID:               id,
		Name:             name,
		CreatedAt:        parseSyfonCreatedAt(record.CreatedTime),
		Size:             record.Size,
		SHA256:           firstSHA256(record.Hashes),
		ControlledAccess: append([]string(nil), record.ControlledAccess...),
		MD5:              checksumOfType(record.Hashes, "md5"),
	}
	for _, method := range record.AccessMethods {
		if method.AccessURL == nil {
			continue
		}
		if value := strings.TrimSpace(method.AccessURL.URL); value != "" {
			file.AccessURL = value
			break
		}
	}
	return file, true
}

func firstSHA256(hashes map[string]string) string {
	values := fileSHA256Hashes(hashes)
	for hash := range values {
		return hash
	}
	return ""
}

func checksumOfType(hashes map[string]string, wanted string) string {
	for kind, value := range hashes {
		if strings.EqualFold(strings.TrimSpace(kind), wanted) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseSyfonCreatedAt(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339, strings.TrimSpace(value))
	return parsed
}
