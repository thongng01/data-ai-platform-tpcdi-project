# Diagram visual systems

The references solve different problems. AWS and Azure icons identify their products and resources. Palantir Blueprint supplies generic interface icons. IBM defines how technical diagrams combine nodes, icons, text, groups, color, and connections. These findings support a project design choice; they do not mean that every vendor convention has been adopted.

The official sources were inspected on 2026-10-28. The comparison is limited to the sources below, rather than a claim about every diagram produced by these companies.

| Reference | What the source defines | Implications |
| --- | --- | --- |
| [AWS architecture icons](https://aws.amazon.com/architecture/icons/) | AWS supplies product icons, resources, and diagram toolkits for architecture diagrams. Its page permits customers and partners to use these assets in diagrams and materials such as presentations and whitepapers. It also warns that third-party libraries can contain legacy icons. | Use the current AWS icon for an AWS service that is actually part of the design. A recognizable service icon conveys identity; it should not substitute for a generic local service. |
| [Azure architecture icons](https://learn.microsoft.com/en-us/azure/architecture/icons/) | Microsoft supplies general-purpose SVGs. It recommends placing the product name close to the icon and keeping the icon as it appears in Azure. It prohibits cropping, flipping, rotating, distorting, or changing the shape, and using Microsoft product icons to represent another product or service. | Use Azure icons for actual Azure services, with their names beside them. Keep neutral shapes and generic icons for the local platform's own services. |
| [Palantir Blueprint icons](https://blueprintjs.com/docs/#icons) | Blueprint documents more than 500 vector UI icons at 16 and 20 pixels, supplied as SVGs and fonts. This is an identifiable official Palantir icon system. It does not establish a general Palantir technical-diagram standard. | Treat Blueprint as an alternative generic icon family. Its UI focus and intended sizes differ from IBM's diagram-specific conventions. Installing its React components is unnecessary for using selected SVG assets. |
| [IBM technical-diagram design](https://www.ibm.com/design/language/infographics/technical-diagrams/design/) | IBM specifies an 8-pixel grid, 24-pixel monochrome UI icons for nodes, 16-pixel badges, left-aligned labels, thin node outlines, and orthogonal routing by default. Icons are optional in larger nodes. Its light-canvas rules use dark text, light fills, and controlled color accents. | These are a complete set of vendor conventions. Taking individual rules from them does not establish compliance with the whole system. |

## Icon and layout considerations

One generic icon family keeps concepts such as a document, processing step, application, and user consistent. Carbon fits the inspected IBM guidance, but it is not required to use native draw.io symbols. Product icons identify named technologies. Common spacing, label placement, and icon size can keep product icons consistent with the surrounding diagram without changing their brand colors.

A processing step needs an icon that describes software work. A processor-chip symbol implies hardware and can misstate that role. If no icon adds meaning, a labeled node is sufficient. An ERD can use native tables, fields, and relationship notation without an icon on every table.

The node layout should remain stable when an icon changes. Color should identify a defined group or state. It should not compensate for unclear labels or uncertain relationships. Large shaded cards, decorative marks, and several unrelated icon families can make a small diagram look busy without adding information.

This approach needs a small set of assets and a few shared layout rules. It does not require a new diagram library, UI framework, or automatic layout system.

## Asset sources and use terms

Microsoft permits its icons in architecture diagrams, training materials, and documentation. Copying, distribution, and display are limited to those permitted uses unless Microsoft grants further permission. The [Azure icon page](https://learn.microsoft.com/en-us/azure/architecture/icons/#icon-terms) states these terms.

The [Carbon repository license](https://github.com/carbon-design-system/carbon/blob/main/LICENSE) and [Blueprint repository license](https://github.com/palantir/blueprint/blob/develop/LICENSE) are Apache License 2.0. When selected assets are copied into the project, retain the applicable license and notices, record the source and version, and identify modifications. This is separate from the trademark identity of any product logo.

The official [Carbon icons package metadata](https://registry.npmjs.org/@carbon/icons/11.91.0) identifies version 11.91.0 and Apache-2.0 licensing. The published package is available from its [npm tarball](https://registry.npmjs.org/@carbon/icons/-/icons-11.91.0.tgz). Reading or copying a few SVGs from that package does not require installing a frontend framework.

## Layout and density references

The [Card Fraud Platform architecture](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/docs/images/platform-architecture.svg) uses a 2166 x 1250 canvas. Its [deployment diagram](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/docs/images/platform-deployment.svg) uses 2240 x 1780 and contains nested host, Docker, Compose, and Kubernetes scopes. Much of their detail needs zooming when displayed at 960 pixels wide. These examples show the effect of content density; they do not define the project's architecture or visual rules.

The [screenshot collection](diagram-references/README.md) contains Azure, Fabric, Databricks, and AWS examples for visual comparison. Discussion history and selection status belong in the [working record](../Status.md).
