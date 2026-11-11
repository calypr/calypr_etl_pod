# Funnel metadata publisher

This image is the source-acquisition worker for selected metadata publication. It downloads only requested files from one immutable GitHub commit, hydrates Git-LFS pointers through the published Git-DRS client, and passes private staged files to the Loom ingestion client. The client begins or resumes one durable source load, then asks the worker to hydrate and upload each missing file in order. The worker removes each staged file after its upload finishes, keeping local disk use near the largest selected file. Loom remains responsible for authoritative project replacement and durable recipe publication.

The worker does not use Git, the old ETL package, or GitHub credentials in `INPUT_DATA`. It accepts only the `run` command, handles `SIGTERM` and `SIGINT`, and exits successfully only after validating a matching `PUBLISHED` Loom receipt.

## Runtime environment

- `INPUT_DATA`: JSON with `projectId`, `organization`, `project`, `repository`, `commit`, and `files`.
- `ACCESS_TOKEN`: Gen3 bearer token for Git-DRS and Loom.
- `GITHUB_TOKEN`: read token used only for GitHub API requests.
- `DRS_URL`: trusted Syfon/Git-DRS base URL.
- `LOOM_URL`: trusted Loom API base URL.
- `ETL_OPERATION_ID`: stable Funnel task ID used as Loom's idempotency key.
- `ETL_WORKSPACE_DIR`: writable per-task volume, normally `/workspace`.

The worker validates project identity, full commit SHA, normalized GitHub HTTPS repository URL, unique direct `META/<FHIRResourceType>.ndjson` paths, service URLs, and operation ID before it makes upstream requests. It permits up to 100 files, with a 1 GiB default per-file limit. Set `ETL_MAX_FILE_BYTES` to a positive byte count at or below 1 GiB to lower that limit. There is no aggregate dataset-size cap. It accepts standard three-line Git-LFS SHA-256 pointers and rejects extensions, placeholders, and DRS-URI pointer forms rather than treating them as NDJSON; Git-DRS owns all payload transfer and integrity verification. GitHub redirects are rejected so the GitHub token cannot be forwarded to another host. DRS retains redirects because Git-DRS may download from a resolved object-store URL. Staged files live in a private job directory, are made read-only before Loom receives them, and are removed after each file upload or when publication returns. The worker exits on `SIGTERM`/`SIGINT` and applies a one-hour job deadline.

## Local build and tests

The worker pins the Loom ingestion client in `go.mod`. Run these commands from `funnel-job`:

```sh
go test ./...
docker build -t calypr-funnel-etl:dev .
```
