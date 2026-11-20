# Funnel worker

Use `run` to load selected FHIR files into Loom’s ArangoDB source store. Use
`publish-dataframe` to publish a saved dataframe to ClickHouse through Loom.

## Load selected metadata

The worker fetches selected files from one pinned GitHub commit, hydrates
Git-LFS pointers through Git-DRS, and passes staged files to Loom. It removes
each staged file after upload. Loom manages the source load and publication.

Selecting `META/DocumentReference.ndjson` also reconciles it against scoped
Syfon records for data pointers in that commit. The worker preserves authored
records and generates missing references for unambiguous matches. If generation
needs a study ID, the commit must contain `META/ResearchStudy.ndjson` with a
study ID. The worker reads that file for context and uploads it only if selected.
Loom generates relationships from the uploaded FHIR records.

The worker does not use Git, the old ETL package, or GitHub credentials in `INPUT_DATA`. The `run` command handles `SIGTERM` and `SIGINT` and exits successfully only after validating a matching `PUBLISHED` Loom receipt.

### Runtime environment

- `INPUT_DATA`: JSON with `projectId`, `organization`, `project`, `repository`, `commit`, and `files`.
- `ACCESS_TOKEN`: Gen3 bearer token for Git-DRS and Loom.
- `GITHUB_TOKEN`: read token used only for GitHub API requests.
- `DRS_URL`: trusted Syfon/Git-DRS base URL.
- `LOOM_URL`: trusted Loom API base URL.
- `ETL_OPERATION_ID`: stable Funnel task ID used as Loom's idempotency key.
- `ETL_WORKSPACE_DIR`: writable per-task volume, normally `/workspace`.

The worker validates project identity, full commit SHA, normalized GitHub HTTPS repository URL, unique direct `META/<FHIRResourceType>.ndjson` paths, service URLs, and operation ID before it makes upstream requests. It permits up to 100 files, with a 1 GiB default per-file limit. Set `ETL_MAX_FILE_BYTES` to a positive byte count at or below 1 GiB to lower that limit. There is no aggregate dataset-size cap. It accepts standard three-line Git-LFS SHA-256 pointers and rejects extensions, placeholders, and DRS-URI pointer forms rather than treating them as NDJSON; Git-DRS owns all payload transfer and integrity verification. GitHub redirects are rejected so the GitHub token cannot be forwarded to another host. DRS retains redirects because Git-DRS may download from a resolved object-store URL. Staged files live in a private job directory, are made read-only before Loom receives them, and are removed after each file upload or when publication returns. The worker exits on `SIGTERM`/`SIGINT` and applies a one-hour job deadline.

## Publish a dataframe

Run `funnel-etl publish-dataframe` with `LOOM_URL`, a Gen3 bearer token in
`ACCESS_TOKEN`, and an `INPUT_DATA` JSON object containing `organization`,
`project`, `explorerId`, and `receiptId`. Loom publishes the saved dataframe
to ClickHouse for the Viewer. On success the worker writes a
`LOOM_PUBLICATION_RESULT` line to the job log.

## Local build and tests

The worker pins the Loom ingestion client in `go.mod`. Run these commands from `funnel-worker`:

```sh
go test ./...
docker build -t calypr-funnel-etl:dev .
```
