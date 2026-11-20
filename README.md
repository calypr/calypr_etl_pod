# Calypr data jobs

| Task | Job | Destination |
| --- | --- | --- |
| Load a complete Git repository snapshot | [`repository-ingest`](repository-ingest/README.md) runs under Sower as `calypr-etl`. It hydrates metadata with Git-DRS and reconciles FHIR DocumentReferences through Syfon. | Loom source data in ArangoDB |
| Load selected FHIR files from a GitHub commit | [`funnel-worker`](funnel-worker/README.md) runs under Funnel with the `run` command. | Loom source data in ArangoDB |
| Publish a saved dataframe for the Viewer | [`funnel-worker`](funnel-worker/README.md#publish-a-dataframe) runs under Funnel with the `publish-dataframe` command. | Published tables in ClickHouse |

The jobs call Loom. Loom owns writes to ArangoDB and ClickHouse. The repository
ingest job can also activate an Explorer from its repository configuration.
