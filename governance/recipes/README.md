# DataHub ingestion recipes

This directory is reserved for metadata ingestion recipes and validation publication code. The intended metadata includes datasets, pipeline lineage, run identity, and validation results. These are separate records: cataloging a table alone does not establish which job produced it or whether its data passed validation.

The governance configuration mounts `governance/` at `/opt/governance`. Its runner names a recipe under `/opt/governance/recipes/`. Recipe names, source connectors, entity mappings, and publication commands must follow the implemented pipeline and storage design. PostgreSQL gold storage remains a proposal, so it is not a fixed connector requirement.

The [project design](../../docs/Project-Design.md#1-coursework-requirements) records the coursework requirements. [Status](../../docs/Status.md#what-exists) records recipe and service readiness, and [IaC](../../docs/IaC.md#45-governance) describes service dependencies.
