# Calypr data jobs

| Task | Job | Destination |
| --- | --- | --- |
| Load selected FHIR files from a GitHub commit | [`funnel-worker`](funnel-worker/README.md) runs under Funnel with the `run` command. | Loom source data in ArangoDB |
| Publish a saved dataframe for the Viewer | [`funnel-worker`](funnel-worker/README.md#publish-a-dataframe) runs under Funnel with the `publish-dataframe` command. | Published tables in ClickHouse |

The Funnel worker calls Loom, which writes to ArangoDB and ClickHouse.
