package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	gitdrs "github.com/calypr/git-drs/client"
	loomingest "github.com/calypr/loom/client/ingest"
)

func main() {
	var err error
	switch {
	case len(os.Args) == 2 && os.Args[1] == "run":
		err = runFromEnvironment()
	case len(os.Args) == 2 && os.Args[1] == "publish-dataframe":
		err = runDataframePublishFromEnvironment(os.Getenv, os.Stdout)
	default:
		fmt.Fprintln(os.Stderr, "usage: funnel-etl run | publish-dataframe")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "funnel publish failed:", redactError(err, strings.TrimSpace(os.Getenv("ACCESS_TOKEN")), strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))))
		os.Exit(1)
	}
}

func runFromEnvironment() error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	config, input, err := loadConfiguration(os.Getenv)
	if err != nil {
		return err
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, jobTimeout)
	defer cancel()
	githubHTTP := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	loomHTTP := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	drsHTTP := &http.Client{}
	drsClient, err := gitdrs.New(gitdrs.Options{
		Endpoint: config.drsURL, AccessToken: config.accessToken,
		Organization: input.Organization, Project: input.Project,
		HTTPClient: drsHTTP,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return fmt.Errorf("configure Git-DRS client: %w", err)
	}
	loomClient, err := loomingest.New(config.loomURL, config.accessToken, loomHTTP)
	if err != nil {
		return fmt.Errorf("configure Loom client: %w", err)
	}
	githubClient, err := newGitHubSource(config.githubToken, githubHTTP)
	if err != nil {
		return fmt.Errorf("configure GitHub API client: %w", err)
	}
	deps := workerDependencies{
		github:    githubClient,
		drs:       drsClient,
		publisher: loomClient,
	}
	receipt, err := runJob(ctx, config, input, deps)
	if err != nil {
		return err
	}
	slog.Info("selected metadata published",
		"operation_id", receipt.OperationID,
		"source_revision", receipt.SourceRevision,
		"manifest_hash", receipt.ManifestHash,
		"execution_id", receipt.ExecutionID,
		"state", receipt.State,
		"reused", receipt.Reused,
	)
	return nil
}

type environment func(string) string

func loadConfiguration(getenv environment) (jobConfig, inputData, error) {
	config := jobConfig{}
	var err error
	if config.accessToken, err = requiredEnvironment(getenv, "ACCESS_TOKEN"); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if config.githubToken, err = requiredEnvironment(getenv, "GITHUB_TOKEN"); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if config.drsURL, err = requiredServiceURL(getenv, "DRS_URL"); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if config.loomURL, err = requiredServiceURL(getenv, "LOOM_URL"); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if config.operationID, err = requiredEnvironment(getenv, "ETL_OPERATION_ID"); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if config.workspaceDir, err = requiredEnvironment(getenv, "ETL_WORKSPACE_DIR"); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if config.maxFileBytes, err = configuredMaxFileBytes(getenv); err != nil {
		return jobConfig{}, inputData{}, err
	}
	if err := validateOperationID(config.operationID); err != nil {
		return jobConfig{}, inputData{}, err
	}

	rawInput, err := requiredEnvironment(getenv, "INPUT_DATA")
	if err != nil {
		return jobConfig{}, inputData{}, err
	}
	input, err := parseInput(rawInput)
	if err != nil {
		return jobConfig{}, inputData{}, err
	}
	if _, err := validateInput(input); err != nil {
		return jobConfig{}, inputData{}, err
	}
	return config, input, nil
}

func configuredMaxFileBytes(getenv environment) (int64, error) {
	raw := strings.TrimSpace(getenv("ETL_MAX_FILE_BYTES"))
	if raw == "" {
		return maxFileBytes, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 || value > maxFileBytes {
		return 0, fmt.Errorf("ETL_MAX_FILE_BYTES must be a positive byte count no greater than %d", maxFileBytes)
	}
	return value, nil
}

func requiredEnvironment(getenv environment, key string) (string, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return value, nil
}

func requiredServiceURL(getenv environment, key string) (string, error) {
	value, err := requiredEnvironment(getenv, key)
	if err != nil {
		return "", err
	}
	return validateServiceURL(value, key)
}

func redactError(err error, secrets ...string) string {
	message := err.Error()
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
