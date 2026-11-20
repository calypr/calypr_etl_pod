package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/calypr/git-drs/inventory"
	"github.com/google/uuid"
)

const fileSHA256System = "https://humantumoratlas.org/FILE_SHA256"

type ndjsonRecord map[string]json.RawMessage

const documentReferencePath = "META/DocumentReference.ndjson"
const researchStudyPath = "META/ResearchStudy.ndjson"

func reconcileStagedDocumentReferences(ctx context.Context, workspace, localPath string, input inputData, repository githubRepository, config jobConfig, deps workerDependencies, limit int64) error {
	inventoryStarted := time.Now()
	pointers, err := fetchGeckoDataPointers(ctx, config.geckoURL, config.accessToken, input)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Git data pointers discovered", "unique_hashes", len(pointers), "duration", time.Since(inventoryStarted))
	hashes := make([]string, 0, len(pointers))
	for hash := range pointers {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	lookupStarted := time.Now()
	objects, err := lookupSyfonFiles(ctx, config.drsURL, config.accessToken, input.Organization, input.Project, hashes)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Syfon hash lookup completed", "hashes", len(hashes), "duration", time.Since(lookupStarted))
	return reconcileDocumentReferenceFile(ctx, localPath, config.drsURL, input.Project, hashes, pointers, objects, limit, func() (string, error) {
		studyWorkspace, err := os.MkdirTemp(workspace, "reference-study-")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(studyWorkspace)
		path, err := fetchAndHydrateFileWithLimits(ctx, studyWorkspace, input.Commit, researchStudyPath, repository, deps.github, deps.drs, sourceLimits{maxFileBytes: limit})
		if err != nil {
			return "", fmt.Errorf("resolve %s for generated DocumentReferences: %w", researchStudyPath, err)
		}
		var id string
		err = visitNDJSONRecords(ctx, path, func(index int, _ []byte, row ndjsonRecord) error {
			if index == 1 && (sonic.ConfigStd.Unmarshal(row["id"], &id) != nil || id == "") {
				return errors.New("ResearchStudy.ndjson needs a record with an id to generate DocumentReferences")
			}
			return nil
		})
		if err != nil {
			return "", err
		}
		if id == "" {
			return "", errors.New("ResearchStudy.ndjson needs a record with an id to generate DocumentReferences")
		}
		return id, nil
	})
}

func reconcileDocumentReferenceFile(
	ctx context.Context, documentPath, endpoint, project string, hashes []string,
	pointers map[string]inventory.Pointer, objects map[string][]syfonFile, limit int64, resolveStudy func() (string, error),
) error {
	started := time.Now()
	namespace := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(endpoint))
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	system := endpoint + "/" + project
	file, err := os.CreateTemp(filepath.Dir(documentPath), ".reconciled-*.ndjson")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	output := bufio.NewWriterSize(file, 64*1024)
	writeRow := func(line []byte) error {
		size := int64(len(line)) + 1
		if size > limit {
			return errors.New("reconciled DocumentReference exceeds the configured size limit")
		}
		limit -= size
		if _, err := output.Write(line); err != nil {
			return err
		}
		return output.WriteByte('\n')
	}
	authoredHashes := make(map[string]bool, len(hashes))
	authored := 0
	err = visitNDJSONRecords(ctx, documentPath, func(_ int, line []byte, row ndjsonRecord) error {
		hash, err := documentReferenceHash(row)
		if err != nil {
			return fmt.Errorf("read authored DocumentReference checksum: %w", err)
		}
		if _, tracked := pointers[hash]; tracked {
			authoredHashes[hash] = true
			if candidates := objects[hash]; len(candidates) == 1 {
				changed, err := setDocumentReferenceDRSID(row, system, candidates[0].ID)
				if err != nil {
					return err
				}
				if changed {
					line, err = sonic.ConfigStd.Marshal(row)
					if err != nil {
						return err
					}
				}
			}
		}
		authored++
		return writeRow(line)
	})
	if err != nil {
		return err
	}

	for _, hash := range hashes {
		if len(objects[hash]) > 1 && !authoredHashes[hash] {
			return fmt.Errorf("Git/Syfon reconciliation found %d records for SHA256 %s", len(objects[hash]), hash)
		}
	}
	var studyID, firstMissingPath string
	missing, generated := 0, 0
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if authoredHashes[hash] {
			continue
		}
		candidates := objects[hash]
		if len(candidates) == 0 {
			if missing == 0 {
				firstMissingPath = pointers[hash].Path
			}
			missing++
			continue
		}
		if studyID == "" {
			studyID, err = resolveStudy()
			if err != nil {
				return err
			}
		}
		record := generatedDocumentReference(namespace, endpoint, project, studyID, hash, pointers[hash], candidates[0])
		line, err := sonic.ConfigStd.Marshal(record)
		if err != nil {
			return err
		}
		if err := writeRow(line); err != nil {
			return err
		}
		generated++
	}
	if err := output.Flush(); err != nil {
		return err
	}
	if err := file.Chmod(0o400); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), documentPath); err != nil {
		return err
	}
	if missing > 0 {
		slog.WarnContext(ctx, "Git pointers without a scoped Syfon record", "count", missing, "first_path", firstMissingPath)
	}
	slog.InfoContext(ctx, "Git/Syfon DocumentReferences reconciled", "authored", authored,
		"generated", generated, "missing_syfon", missing, "duration", time.Since(started))
	return nil
}

func generatedDocumentReference(namespace uuid.UUID, endpoint, project, studyID, hash string, pointer inventory.Pointer, object syfonFile) map[string]any {
	name := strings.TrimSpace(object.Name)
	if name == "" {
		name = filepath.Base(pointer.Path)
	}
	extensionBase := endpoint + "/fhir/StructureDefinition/"
	extensions := []map[string]string{
		{"url": extensionBase + "checksum-sha256", "valueString": hash},
		{"url": extensionBase + "source", "valueString": "s3"},
	}
	if object.MD5 != "" {
		extensions = append(extensions, map[string]string{"url": extensionBase + "checksum-md5", "valueString": object.MD5})
	}
	attachment := map[string]any{"title": name, "size": strconv.FormatInt(pointer.Size, 10), "extension": extensions}
	if object.AccessURL != "" {
		attachment["url"] = object.AccessURL
	}
	row := map[string]any{
		"resourceType": "DocumentReference",
		"id":           uuid.NewSHA1(namespace, []byte(project+"/sha256:"+hash)).String(),
		"status":       "current",
		"docStatus":    "final",
		"subject":      map[string]string{"reference": "ResearchStudy/" + studyID},
		"identifier": []map[string]string{
			{"use": "official", "system": endpoint + "/" + project, "value": object.ID},
			{"system": fileSHA256System, "value": hash},
		},
		"content": []map[string]any{{"attachment": attachment}},
	}
	if !object.CreatedAt.IsZero() {
		date := object.CreatedAt.Format(time.RFC3339)
		row["date"] = date
		attachment["creation"] = date
	}
	return row
}

func documentReferenceHash(row ndjsonRecord) (string, error) {
	var fields struct {
		Content []struct {
			Attachment struct {
				Extension []struct {
					URL         string `json:"url"`
					ValueString string `json:"valueString"`
				} `json:"extension"`
			} `json:"attachment"`
		} `json:"content"`
		Identifier []struct {
			System string `json:"system"`
			Value  string `json:"value"`
		} `json:"identifier"`
	}
	if content := row["content"]; len(content) > 0 {
		if err := sonic.ConfigStd.Unmarshal(content, &fields.Content); err != nil {
			return "", err
		}
	}
	if len(fields.Content) > 0 {
		for _, extension := range fields.Content[0].Attachment.Extension {
			if extension.ValueString != "" && (strings.HasSuffix(extension.URL, "/checksum-sha256") || strings.HasSuffix(extension.URL, "/sha256")) {
				return normalizeDocumentHash(extension.ValueString), nil
			}
		}
	}
	if identifiers := row["identifier"]; len(identifiers) > 0 {
		if err := sonic.ConfigStd.Unmarshal(identifiers, &fields.Identifier); err != nil {
			return "", err
		}
	}
	for _, identifier := range fields.Identifier {
		if strings.Contains(strings.ToUpper(identifier.System), "SHA256") {
			return normalizeDocumentHash(identifier.Value), nil
		}
	}
	return "", nil
}

func normalizeDocumentHash(value string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "sha256:"))
}

func setDocumentReferenceDRSID(row ndjsonRecord, system, id string) (bool, error) {
	var identifiers []json.RawMessage
	if err := sonic.ConfigStd.Unmarshal(row["identifier"], &identifiers); err != nil && len(row["identifier"]) != 0 {
		return false, fmt.Errorf("read authored DocumentReference identifiers: %w", err)
	}
	next := make([]json.RawMessage, 1, len(identifiers)+1)
	for index, raw := range identifiers {
		var identifier struct {
			Use, System, Value string
		}
		if err := sonic.ConfigStd.Unmarshal(raw, &identifier); err != nil {
			return false, err
		}
		if index == 0 && identifier.Value == id {
			return false, nil
		}
		if identifier.Use != "official" && identifier.System != system {
			next = append(next, raw)
		}
	}
	official, err := sonic.ConfigStd.Marshal(map[string]string{"use": "official", "system": system, "value": id})
	if err != nil {
		return false, err
	}
	next[0] = official
	row["identifier"], err = sonic.ConfigStd.Marshal(next)
	return true, err
}

func visitNDJSONRecords(ctx context.Context, path string, visit func(index int, line []byte, row ndjsonRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	rowNumber := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		rowNumber++
		var record ndjsonRecord
		if err := sonic.ConfigStd.Unmarshal(line, &record); err != nil {
			return fmt.Errorf("decode %s row %d: %w", filepath.Base(path), rowNumber, err)
		}
		if record == nil {
			return fmt.Errorf("decode %s row %d: expected a JSON object", filepath.Base(path), rowNumber)
		}
		if err := visit(rowNumber, line, record); err != nil {
			return err
		}
	}
	return scanner.Err()
}
