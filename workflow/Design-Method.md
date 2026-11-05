# Design Method

A design choice in this project moves through six steps: frame, map, research, generate options, evaluate, and record. Each section uses a decision from [Data Platform](../docs/Data-Platform.md#6-layer-1-design-decisions-and-rationale) as its example. The [documentation guide](Documentation-Guide.md#4-recording-design-decisions) defines how to record the result.

## 1. The Six Steps

| Step | Question | Output | Example: Object Storage Buckets |
| --- | --- | --- | --- |
| **1. Frame** | What exactly must be decided, what depends on it, and must it be decided now? | One question, its dependents, and a now-or-later call | Which data gets its own bucket? Prefixes, dataset scope, and credentials depend on it. Raw data is costly to move later, so decide now. |
| **2. Map** | Who writes and reads the data, what are its attributes, and which constraints are fixed? | Attribute table and constraint list | Writer, readers, rebuild cost, and lifetime of each kind of stored data |
| **3. Research** | What do the specification, reference code, and platforms do, and what reasons do they give? | Sources with their reasons | AWS separates layers because access, versioning, and encryption can differ by layer. |
| **4. Generate** | What are the simplest option, the heaviest option, and the options between? | Three or more options | One bucket; one bucket per zone; buckets by purpose |
| **5. Evaluate** | Which option meets every criterion with the least added complexity? | A choice, rejected options with reasons, and a revisit trigger | Test with scenarios: a job clears the wrong prefix; a dataset is regenerated. |
| **6. Record** | What was chosen, by whom, why, and what remains open? | Decision row and rationale section | [Data Platform Section 6.2](../docs/Data-Platform.md#62-object-storage-layout-and-dataset-scope) |

The steps repeat when needed. Research can expose a missing attribute, and evaluation can show that the question was framed wrong.

## 2. Frame the Decision

- **One question per decision**: Split a broad question into parts that have separate answers. "How do source files arrive?" became date meaning, time zone, calendar, completeness, buckets, dataset isolation, prefixes, and reruns.
- **Order by dependency**: Answer first the question that constrains the others. The date meaning came before the time zone, and buckets came before prefixes.
- **Decide now or defer**: Decide now when other work depends on the answer or when reversing it is costly. Defer when nothing depends on it yet, and record what brings it back. Archive and retention policies wait for the lifecycle design; HR and reference updates wait until a feature needs them.
- **Challenge the framing**: A question can contain a wrong assumption. "Which control file format?" assumed that the provider sends a control file. The better question was how ingestion knows that a delivery is complete. Similarly, "How do we isolate datasets?" assumed that several datasets must coexist; they do not, because every planned comparison uses one timeline.

## 3. Map the Problem

List the attributes that decide the design before listing solutions.

| Area | Questions | Example |
| --- | --- | --- |
| **Source and delivery** | Who produces the data, in what format, and at what cadence? What does the provider not send? | Providers send data files only, so completeness uses a pipeline-side file list. |
| **Time** | Which clocks exist: event, business date, publication, extraction, arrival, processing? Which time zone and day boundary apply? | A Q1 report has a reporting period, a publication date, and an arrival time. |
| **Identity** | What identifies an entity, a version, an event, and a delivery? | A repeated cash event keeps its Event ID; a Kafka offset identifies only a delivery position. |
| **State and order** | Does a step depend on earlier results? | SCD2 versions and cash balances require dates in order. |
| **Storage** | Can the data be rebuilt? Who writes it, and how long is it kept? | Raw files stand for provider deliveries, which cannot be rebuilt, so they get their own bucket. |
| **Scale and shape** | How large is the data, how skewed, and how many distinct values does a key have? | At least 100 GB, a skewed key, and measured high-cardinality columns |
| **Failure** | What can be partial, duplicated, late, or retried? | S3 writes each object completely, but a delivery appears one file at a time. |
| **Evidence** | How will we show that the result is correct? | The generator summary records measured values, not only settings. |
| **Change and correction** | Can this be rerun safely? What must rerun when an input changes? How do we show what changed? | A corrected customer file triggers a rerun of its date and every later date; Bronze keeps both deliveries. |
| **Lifecycle** | How long is this kept, and what removes it? How are new versions of code and models released? | The raw and clean cash topics keep the whole dataset; the features and rescore topics keep 7 days. |
| **Security and privacy** | Who may read or change this? Does it hold personal data? What could an attacker send into it, such as SQL or prompt injection? | Customer names, addresses, tax IDs, emails, and phone numbers are personal data. |
| **Operations** | How do we know it works? How do we recover it after a failure? | `ops.load_status` and its completeness view show which sources each date is missing. |
| **Engineering quality** | How is it tested? Can someone else reproduce it? What does it cost to run? | The same configuration and seed reproduce the same source files byte for byte; each training run saves its dataset with the model in MLflow. |

Classify each constraint before treating it as fixed:
- **Fundamental**: Fixed by a specification or a system property. S3 cannot write several objects as one operation.
- **Practical**: Set by current tools, time, or hardware, and possibly changeable. SeaweedFS default volume settings make each bucket costly; two settings reduce the cost.
- **Preference**: Either answer works. The name `staging` matches TPC-DI, but another name would also work.

## 4. Research Reference Designs

- **Primary sources first**: Use the TPC-DI specification, official vendor documentation, reference code, and engine documentation. A summary can drop the conditions under which advice holds.
- **Record the reason with the practice**: A practice transfers only when its reason applies here. AWS separates every layer because their requirements can differ. Bronze and Silver share their requirements here, so a prefix separates them.
- **Separate benchmark artifacts from real behavior**: `BatchDate.txt` exists for benchmark convenience. Real extracts carry their date in the file name or path.
- **Compare platforms by purpose, not by name**: A Fabric workspace, an Azure storage account, and an S3 bucket serve the same purpose. Vendors also use one word for different things: AWS "stage" data is processed, while our Staging holds raw files.

## 5. Generate Options

- **Cover the range**: Include the simplest option, the option that large platforms use, and the options between. Keep the benchmark behavior as an option.
- **Split independent parts**: FINWIRE needed two choices: its release schedule and the meaning of its file quarter.

| FINWIRE Cadence Option | Added Complexity | Result |
| --- | --- | --- |
| Historical only, as in TPC-DI | None | Company, security, and financial data stay unchanged for a year. |
| Fixed release date after each quarter | Low | Delayed availability from one rule |
| Staggered delays per company | Medium | Closer to real reporting, with more rules and checks |
| Detailed filing calendars | High | Detail that account attrition does not use |

## 6. Evaluate and Choose

- **Derive criteria from the mapped attributes**: Words such as "scalable" or "flexible" cannot separate options. "Stays stable when a delivery is late or retried" decided the meaning of `data_date`.
- **Walk each option through concrete scenarios**:
  - A weekend date with no market activity
  - A Q1 report published April 30, used for a P/E value on April 15
  - A delivery that arrives with one file missing
  - A job that clears the wrong prefix
  - A dataset regenerated with new settings
- **Choose the simplest option that meets every criterion.**
- **State each simplification and its cost**: All FINWIRE postings in a quarter release together, so new securities appear at most once a quarter.
- **Name the revisit trigger**: Bronze and Silver share a bucket until they need different retention or access.

## 7. Balance Simplicity and Need

Add complexity only for one of these reasons:

| Reason | Example |
| --- | --- |
| A requirement needs it | A stable Event ID, so a duplicated streaming event counts once |
| It prevents a likely and costly failure | A separate bucket for raw files that cannot be rebuilt |
| It is cheap now and costly later | Bucket boundaries chosen before data exists |
| Realism changes how the system must behave | Delayed FINWIRE availability creates point-in-time rules for P/E values and features. |

- **Signs of too much**: A part that nobody reads or checks, such as computing our own hashes over 100 GB of files when no step compares them; a boundary without a difference in requirements, such as a bucket per zone; generality for a scale nobody needs.
- **Signs of too little**: One field with two meanings, such as one date for both the extraction day and the data day; no way to detect an incomplete delivery; an unstated assumption, such as treating the latest record date as the historical cutoff.

## 8. Rules From This Project

| Rule | Where It Applied |
| --- | --- |
| Give each time concept its own field | `data_date` versus extraction and arrival; reporting period versus publication and availability |
| Rely only on what the source actually sends | Pipeline-side expected-file list; no provider control file |
| Protect what cannot be rebuilt; rebuild the rest | `staging` bucket; Bronze and Silver share `lakehouse` |
| Question where a need came from before building for it | One active dataset; no dataset level in storage |
| Start from a baseline that existing mechanisms provide; add mechanisms when a requirement appears | Data versioning: retained source versions, provenance, Delta history, and SCD2; add lifecycle rules when their requirements are defined |
| Never use data before it was available | FINWIRE availability; point-in-time ML features |
| Identify events by stable facts, not by delivery position | Event ID across repeated deliveries |
| Separate configured targets from measured results | Generator summary |
| Keep reference outputs out of the ingestion path | Generator summary stays on local disk |
| Use boundaries where requirements differ and organization where they match | Buckets versus prefixes |
| Decide costly-to-reverse choices first | Buckets before prefixes; retention periods deferred |
