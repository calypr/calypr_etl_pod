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

func (j *job) reconcileDocumentReferences(repository, directory string) error {
	pointers, err := inventory.List(j.ctx, inventory.Options{
		RepositoryRoot: repository, Ref: j.input.GHCommitHash,
		ExcludePrefixes: []string{metaDir, configDir},
	})
	if err != nil {
		return err
	}
	firstPointer := make(map[string]inventory.Pointer)
	paths := make(map[string][]string)
	for _, pointer := range pointers {
		if _, exists := firstPointer[pointer.SHA256]; !exists {
			firstPointer[pointer.SHA256] = pointer
		}
		paths[pointer.SHA256] = append(paths[pointer.SHA256], pointer.Path)
	}
	hashes := make([]string, 0, len(firstPointer))
	for hash := range firstPointer {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)

	endpoint := j.input.APIEndpoint
	objects, err := lookupSyfonFiles(j.ctx, endpoint, j.token, j.program, j.project, hashes)
	if err != nil {
		return err
	}
	return reconcileDocumentReferenceFiles(j.ctx, directory, endpoint, j.project, hashes, firstPointer, paths, objects)
}

func reconcileDocumentReferenceFiles(
	ctx context.Context, directory, endpoint, project string, hashes []string,
	pointers map[string]inventory.Pointer, paths map[string][]string, objects map[string][]syfonFile,
) error {
	documentPath := filepath.Join(directory, "DocumentReference.ndjson")
	authored, err := readNDJSONRecords(documentPath)
	if err != nil {
		return err
	}
	namespace := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(endpoint))
	studySystem := endpoint + "/" + project
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	system := endpoint + "/" + project
	authoredHashes := make(map[string]bool)
	for _, row := range authored {
		hash, err := documentReferenceHash(row)
		if err != nil {
			return fmt.Errorf("read authored DocumentReference checksum: %w", err)
		}
		if hash == "" {
			continue
		}
		authoredHashes[hash] = true
		if candidates := objects[hash]; len(candidates) == 1 {
			if err := setDocumentReferenceDRSID(row, system, candidates[0].ID); err != nil {
				return err
			}
		}
	}

	for _, hash := range hashes {
		if len(objects[hash]) > 1 && !authoredHashes[hash] {
			return fmt.Errorf("Git/Syfon reconciliation found %d records for SHA256 %s", len(objects[hash]), hash)
		}
	}

	studyID, err := ensureResearchStudy(directory, namespace, studySystem, project)
	if err != nil {
		return err
	}
	records := make([]any, 0, len(authored)+len(hashes))
	for _, row := range authored {
		records = append(records, row)
	}
	missing := 0
	for _, hash := range hashes {
		if authoredHashes[hash] {
			continue
		}
		candidates := objects[hash]
		if len(candidates) == 0 {
			missing++
			slog.WarnContext(ctx, "Git pointers without a scoped Syfon record", "sha256", hash, "paths", paths[hash])
			continue
		}
		record := generatedDocumentReference(namespace, endpoint, project, studyID, hash, pointers[hash], candidates[0])
		records = append(records, record)
	}
	slog.InfoContext(ctx, "Git/Syfon DocumentReferences reconciled", "authored", len(authored),
		"generated", len(records)-len(authored), "missing_syfon", missing)
	return writeNDJSONRecords(documentPath, records)
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

func setDocumentReferenceDRSID(row ndjsonRecord, system, id string) error {
	var identifiers []json.RawMessage
	if err := sonic.ConfigStd.Unmarshal(row["identifier"], &identifiers); err != nil && len(row["identifier"]) != 0 {
		return fmt.Errorf("read authored DocumentReference identifiers: %w", err)
	}
	official, err := sonic.ConfigStd.Marshal(map[string]string{"use": "official", "system": system, "value": id})
	if err != nil {
		return err
	}
	next := []json.RawMessage{official}
	for index, raw := range identifiers {
		var identifier struct {
			Use, System, Value string
		}
		if err := sonic.ConfigStd.Unmarshal(raw, &identifier); err != nil {
			return err
		}
		if index == 0 && identifier.Value == id {
			return nil
		}
		if identifier.Use != "official" && identifier.System != system {
			next = append(next, raw)
		}
	}
	row["identifier"], err = sonic.ConfigStd.Marshal(next)
	return err
}

func ensureResearchStudy(directory string, namespace uuid.UUID, system, project string) (string, error) {
	path := filepath.Join(directory, "ResearchStudy.ndjson")
	rows, err := readNDJSONRecords(path)
	if err != nil {
		return "", err
	}
	if len(rows) > 0 {
		var id string
		if err := sonic.ConfigStd.Unmarshal(rows[0]["id"], &id); err != nil || id == "" {
			return "", fmt.Errorf("authored ResearchStudy is missing an id")
		}
		if _, exists := rows[0]["rootDir"]; exists {
			delete(rows[0], "rootDir")
			records := make([]any, 0, len(rows))
			for _, row := range rows {
				records = append(records, row)
			}
			if err := writeNDJSONRecords(path, records); err != nil {
				return "", err
			}
		}
		return id, nil
	}
	if _, err := os.Stat(path); err == nil {
		return "", fmt.Errorf("authored ResearchStudy file is empty")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	id := uuid.NewSHA1(namespace, []byte(project+"/"+system)).String()
	return id, writeNDJSONRecords(path, []any{map[string]any{
		"resourceType": "ResearchStudy", "id": id, "status": "active",
		"description": "Skeleton ResearchStudy for " + project,
		"identifier":  []map[string]string{{"use": "official", "system": system, "value": project}},
	}})
}

func readNDJSONRecords(path string) ([]ndjsonRecord, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var records []ndjsonRecord
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record ndjsonRecord
		if err := sonic.ConfigStd.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("decode %s row %d: %w", filepath.Base(path), len(records)+1, err)
		}
		if record == nil {
			return nil, fmt.Errorf("decode %s row %d: expected a JSON object", filepath.Base(path), len(records)+1)
		}
		records = append(records, record)
	}
	return records, scanner.Err()
}

func writeNDJSONRecords(path string, rows []any) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".reconciled-*.ndjson")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encoder := sonic.ConfigStd.NewEncoder(file)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
