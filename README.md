# Calypr ETL jobs

This repository builds two Go container images that work with Loom.

| Job | What it does |
| --- | --- |
| Repository ETL | Sower runs `calypr-etl` to load a Git snapshot. The job hydrates metadata with Git-DRS, reconciles FHIR DocumentReferences through Syfon, and sends the snapshot to Loom. |
| Funnel worker | `funnel-etl run` loads selected FHIR files from a pinned GitHub commit. `funnel-etl publish-dataframe` asks Loom to publish a saved dataframe for the Viewer. |

See the [repository ETL guide](etl-job/README.md) and the [Funnel worker guide](funnel-job/README.md) for configuration and local checks.
