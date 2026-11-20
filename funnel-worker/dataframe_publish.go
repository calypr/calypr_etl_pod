package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/calypr/loom/client/authoring"
)

const dataframePublishTimeout = 2 * time.Hour

var dataframePublishIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type dataframePublishInput struct {
	Organization string `json:"organization"`
	Project      string `json:"project"`
	ExplorerID   string `json:"explorerId"`
	ReceiptID    string `json:"receiptId"`
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
	client, err := authoring.New(loomURL, token, &http.Client{Timeout: dataframePublishTimeout})
	if err != nil {
		return fmt.Errorf("configure Loom authoring client: %w", err)
	}
	result, err := client.Publish(ctx, authoring.PublishRequest{
		Project:          input.Organization + "/" + input.Project,
		ExplorerID:       input.ExplorerID,
		AuthResourcePath: "/programs/" + input.Organization + "/projects/" + input.Project,
		ReceiptID:        input.ReceiptID,
	})
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
