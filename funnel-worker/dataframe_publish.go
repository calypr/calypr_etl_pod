package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const dataframePublishTimeout = 2 * time.Hour

var dataframePublishIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type dataframePublishInput struct {
	Organization string `json:"organization"`
	Project      string `json:"project"`
	ExplorerID   string `json:"explorerId"`
	ReceiptID    string `json:"receiptId"`
}

type dataframePublishResponse struct {
	ReceiptID  string `json:"receiptId"`
	RevisionID string `json:"revisionId"`
	State      string `json:"state"`
}

func runDataframePublishFromEnvironment(getenv environment, output io.Writer) error {
	token, err := requiredEnvironment(getenv, "ACCESS_TOKEN")
	if err != nil {
		return err
	}
	loomURL, err := requiredServiceURL(getenv, "LOOM_URL")
	if err != nil {
		return err
	}
	rawInput, err := requiredEnvironment(getenv, "INPUT_DATA")
	if err != nil {
		return err
	}
	input, err := parseDataframePublishInput(rawInput)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), dataframePublishTimeout)
	defer cancel()
	client := &http.Client{
		Timeout: dataframePublishTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	result, err := publishDataframe(ctx, client, loomURL, token, input)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "LOOM_PUBLICATION_RESULT %s\n", result)
	return err
}

func parseDataframePublishInput(raw string) (dataframePublishInput, error) {
	if len(raw) > maxInputDataBytes {
		return dataframePublishInput{}, errors.New("INPUT_DATA exceeds the size limit")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var input dataframePublishInput
	if err := decoder.Decode(&input); err != nil {
		return dataframePublishInput{}, fmt.Errorf("decode INPUT_DATA: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return dataframePublishInput{}, errors.New("INPUT_DATA must contain one JSON object")
	}
	for label, value := range map[string]string{
		"organization": input.Organization,
		"project":      input.Project,
		"explorerId":   input.ExplorerID,
		"receiptId":    input.ReceiptID,
	} {
		if !validProjectComponent(value) || !dataframePublishIdentifier.MatchString(value) {
			return dataframePublishInput{}, fmt.Errorf("%s must be a valid Loom identifier", label)
		}
	}
	return input, nil
}

func publishDataframe(ctx context.Context, client *http.Client, loomURL, token string, input dataframePublishInput) (json.RawMessage, error) {
	base, err := url.Parse(loomURL)
	if err != nil {
		return nil, errors.New("LOOM_URL is invalid")
	}
	projectPath := url.PathEscape(url.PathEscape(input.Organization + "/" + input.Project))
	explorerPath := url.PathEscape(input.ExplorerID)
	escapedPath := strings.TrimRight(base.EscapedPath(), "/") + "/api/v1/projects/" + projectPath + "/explorers/" + explorerPath + "/authoring/v2/publish"
	endpoint, err := url.Parse(base.Scheme + "://" + base.Host + escapedPath)
	if err != nil {
		return nil, fmt.Errorf("build Loom publication URL: %w", err)
	}
	query := endpoint.Query()
	query.Set("auth_resource_path", "/programs/"+input.Organization+"/projects/"+input.Project)
	endpoint.RawQuery = query.Encode()
	body, err := json.Marshal(struct {
		ReceiptID string `json:"receiptId"`
	}{ReceiptID: input.ReceiptID})
	if err != nil {
		return nil, fmt.Errorf("encode Loom publication request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Loom publication request: %w", err)
	}
	req.Header.Set("Authorization", "bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request Loom publication: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("Loom publication returned HTTP %s: %w", response.Status, safeLoomResponseError(response))
	}
	var raw json.RawMessage
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode Loom publication response: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("Loom publication response contains trailing data")
	}
	var result dataframePublishResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode Loom publication response: %w", err)
	}
	if result.ReceiptID != input.ReceiptID || strings.TrimSpace(result.RevisionID) == "" || result.State != "ACTIVE" {
		return nil, errors.New("Loom returned a mismatched receipt, missing revision, or inactive publication")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, fmt.Errorf("compact Loom publication response: %w", err)
	}
	return compact.Bytes(), nil
}

func safeLoomResponseError(response *http.Response) error {
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&envelope); err != nil {
		return errors.New("Loom did not return a readable publication error")
	}
	message := strings.TrimSpace(envelope.Error.Message)
	if message == "" {
		message = "publication request failed"
	}
	if len(message) > 512 {
		message = message[:512]
	}
	parts := []string{message}
	if envelope.Error.Code != "" {
		parts = append(parts, "code="+envelope.Error.Code)
	}
	if envelope.Error.RequestID != "" {
		parts = append(parts, "request_id="+envelope.Error.RequestID)
	}
	return errors.New(strings.Join(parts, " "))
}
