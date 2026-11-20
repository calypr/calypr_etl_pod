# Repository ETL

Sower starts this job with the `fhir_import_export` action. The image runs the
`calypr-etl` Go executable.

## Runtime configuration

Sower supplies `ACCESS_TOKEN`, `GEN3_HOSTNAME`, and `INPUT_DATA`. For a
repository load, set `method` to `put` in `INPUT_DATA` and supply
`projectId`, `ghRepoUrl`, `ghCommitHash`, `ghUserName`, `ghToken`,
`profile`, and `APIEndpoint`. Keep credentials in the job's secret inputs.

The job checks out the specified commit, hydrates metadata with Git-DRS,
and reconciles DocumentReferences against Syfon within the project. It sends
the FHIR snapshot to Loom. Loom generates relationships and activates the
generation after validation and materialization.

If the repository has `CONFIG/<project>.json`, the job sends it to Loom as
an authored `ExplorerConfigV2`. Otherwise, Loom uses the registered default
recipe.

| Optional setting | Purpose |
| --- | --- |
| `LOOM_URL` | Overrides the Loom URL derived from `GEN3_HOSTNAME`. |
| `LOOM_RECIPE_NAME` | Overrides `calypr-meta-default` for jobs without repository Explorer configuration. |
| `LOOM_RECIPE_OUTPUTS` | Sets the comma-separated list of required outputs for the registered recipe. |
| `LOOM_TRANSLATION_VERSION` | Requires a specific version of the registered recipe. |
| `LOOM_REQUIRED_DATAFRAME_SELECTORS` | Supplies exact recipe and output selectors as a JSON array. |

Loom uses the Git commit as the generation key. To rebuild Loom-derived state
for the same commit, set `forceLoomRefresh` to `true` in `INPUT_DATA`. The job
uses a fresh generation for that run.

## Build and test locally

Run these commands from `etl-job`.

```sh
go test ./...
go vet ./...
docker build -t calypr-etl:dev .
```
