# Diagram examples

These examples apply the [diagram guide](Diagram-Guide.md) to source flows, deployment scopes, and data relationships. The PNG views use the standard 1920 x 1440 export. The larger deployment master uses SVG for separate viewing and zooming.

## Data integration

This view separates TPC-DI source origins from extracted files, integration work, and warehouse output. The lower band shows the historical load followed by two incremental updates. It summarizes the conceptual environment in Clause 1 of the [TPC-DI specification](TPC-DI/TPC-DI_v1.1.0.pdf); individual source files and warehouse tables are outside this view.

![TPC-DI sources feed the staging area, integration system, and warehouse.](diagrams/style-examples/data-integration.png)

[Edit the data-integration view.](diagrams/style-examples/data-integration.drawio)

## Deployment

This illustrative deployment places ingestion and processing on two virtual machines. Solid boundaries show the subscription, machines, and service groups. The dashed boundary names the virtual network. Solid arrows show data or requests; dashed arrows show scheduling. Shared services appear below the main flow. The Azure product icons identify the actual services named in this example.

The diagram illustrates notation and layout. It does not define the coursework architecture, a production deployment recipe, or all required network connections. The batch loader sends rows to the transform worker and keeps raw files. The load from curated files to PostgreSQL summarizes a publishing operation whose implementation is outside this view.

[Open the 1440 x 960 deployment master as SVG.](diagrams/style-examples/analytics-deployment.svg) View it at its native width or zoom in to read the complete detail.

[Edit the deployment master.](diagrams/style-examples/analytics-deployment.drawio)

## Data relationships

This illustrative model uses native tables, fields, and crow's-foot connectors. A source system can have zero or more batches. A batch can have zero or more rejected records. Each batch belongs to one source system, and each rejected record belongs to one batch. These example tables do not define the TPC-DI data model.

![Source-system, load-batch, and rejected-record tables with field-level relationships.](diagrams/style-examples/record-relationships.png)

[Edit the record relationships.](diagrams/style-examples/record-relationships.drawio)
