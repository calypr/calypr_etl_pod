package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/calypr/syfon/apigen/internalapi"
)

const syfonHashBatchSize = 500
const maxSyfonResponseBytes = 8 << 20

type syfonFile struct {
	ID        string
	Name      string
	AccessURL string
	MD5       string
	CreatedAt time.Time
}

type syfonHashResponse struct {
	Results map[string][]syfonHashRecord `json:"results"`
	Records []syfonHashRecord            `json:"records"`
}

type syfonHashRecord struct {
	internalapi.InternalRecord
	FileName string `json:"file_name"`
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
	base.Path = strings.TrimRight(base.Path, "/") + "/"

	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	options := []internalapi.ClientOption{internalapi.WithHTTPClient(httpClient)}
	if accessToken := strings.TrimSpace(token); accessToken != "" {
		options = append(options, internalapi.WithRequestEditorFn(func(_ context.Context, request *http.Request) error {
			request.Header.Set("Authorization", "Bearer "+accessToken)
			return nil
		}))
	}
	syfon, err := internalapi.NewClient(base.String(), options...)
	if err != nil {
		return nil, fmt.Errorf("configure Syfon API client: %w", err)
	}

	seen := make(map[string]map[string]struct{}, len(requestHashes))
	for hash := range requestedSet {
		seen[hash] = make(map[string]struct{})
	}

	for start := 0; start < len(requestHashes); start += syfonHashBatchSize {
		end := min(start+syfonHashBatchSize, len(requestHashes))
		batch := requestHashes[start:end]
		response, err := syfon.InternalBulkHashes(ctx, internalapi.BulkHashesRequest{Hashes: prefixedSHA256Hashes(batch)})
		if err != nil {
			return nil, fmt.Errorf("query Syfon objects by checksum: %w", err)
		}
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, maxSyfonResponseBytes+1))
		closeErr := response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read Syfon hash lookup response: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close Syfon hash lookup response: %w", closeErr)
		}
		if int64(len(payload)) > maxSyfonResponseBytes {
			return nil, fmt.Errorf("Syfon hash lookup response exceeds %d bytes", maxSyfonResponseBytes)
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
			if !syfonRecordMatchesScope(record.InternalRecord, organization, project) {
				continue
			}
			file, ok := syfonFileFromRecord(record.InternalRecord)
			if !ok {
				continue
			}
			if file.Name == "" {
				file.Name = strings.TrimSpace(record.FileName)
			}
			for hash := range fileSHA256Hashes(hashInfo(record.Hashes)) {
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

func syfonRecordMatchesScope(record internalapi.InternalRecord, organization, project string) bool {
	recordOrganization := ""
	if record.Organization != nil {
		recordOrganization = strings.TrimSpace(*record.Organization)
	}
	recordProject := ""
	if record.Project != nil {
		recordProject = strings.TrimSpace(*record.Project)
	}
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
	if record.ControlledAccess == nil {
		return strings.TrimSpace(organization) == "" && strings.TrimSpace(project) == ""
	}
	for _, scope := range *record.ControlledAccess {
		if strings.TrimSpace(scope) == expected {
			return true
		}
	}
	return strings.TrimSpace(organization) == "" && strings.TrimSpace(project) == ""
}

func syfonFileFromRecord(record internalapi.InternalRecord) (syfonFile, bool) {
	id := strings.TrimSpace(record.Did)
	if id == "" {
		return syfonFile{}, false
	}
	name := strings.TrimSpace(pointerString(record.Name))
	file := syfonFile{
		ID:        id,
		Name:      name,
		CreatedAt: parseSyfonCreatedAt(pointerString(record.CreatedTime)),
		MD5:       checksumOfType(hashInfo(record.Hashes), "md5"),
	}
	if record.AccessMethods != nil {
		for _, method := range *record.AccessMethods {
			if method.AccessUrl == nil {
				continue
			}
			if value := strings.TrimSpace(method.AccessUrl.Url); value != "" {
				file.AccessURL = value
				break
			}
		}
	}
	return file, true
}

func checksumOfType(hashes map[string]string, wanted string) string {
	for kind, value := range hashes {
		if strings.EqualFold(strings.TrimSpace(kind), wanted) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func hashInfo(value *internalapi.HashInfo) map[string]string {
	if value == nil {
		return nil
	}
	return map[string]string(*value)
}

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func parseSyfonCreatedAt(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339, strings.TrimSpace(value))
	return parsed
}
