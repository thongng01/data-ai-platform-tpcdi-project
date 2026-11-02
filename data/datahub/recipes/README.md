# DataHub ingestion recipes

This directory is reserved for metadata ingestion recipes and validation publication code. The intended metadata includes datasets, pipeline lineage, run identity, and validation results. These are separate records: cataloging a table alone does not establish which job produced it or whether its data passed validation.

The governance configuration mounts `data/datahub/` at `/opt/governance`. Its runner names a recipe under `/opt/governance/recipes/`. Recipe names, source connectors, entity mappings, and publication commands follow the pipeline and storage design in [Data Platform](../../../docs/Data-Platform.md#5-orchestration-quality-and-governance). [IaC](../../../docs/IaC.md#45-governance) describes service dependencies.
