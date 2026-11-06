# Brokerage Source Publisher

The publisher implements generation stage 4. It reads a saved generator dataset, uploads its complete offline files to the source department's `upstream` bucket, and sends its saved cash transactions to `brokerage.cash_transaction.raw`. It does not run the simulation, change source-recording data errors, or send generator metadata and hidden state.

The [generator guide](../generator/README.md) defines the saved files. [Data Platform section 2](../../../docs/Data-Platform.md#2-data-generation-and-problem-injection) defines generation and delivery. [Data Specification](../../../docs/Data-Specification.md#3-physical-source-files-and-ingestion-rules) defines file names and cadence.

## Commands

Run commands from the repository root on Linux with Python 3.14. Preflight and help use only the Python standard library:

```bash
.venv/bin/python -m data.data_source.publisher --help
.venv/bin/python -m data.data_source.publisher check local/runs/small
```

Install network adapters in a separate environment. This keeps publisher dependencies separate from generator dependencies:

```bash
uv venv --python 3.14 local/tmp/publisher-env
uv pip install --python local/tmp/publisher-env/bin/python \
  -r data/data_source/publisher/requirements.txt
```

Supply destination addresses through flags or environment variables. Supply AWS credentials through the external AWS credential chain, such as environment variables or a private AWS profile. Supply Kafka authentication and TLS settings through a private external JSON file. Never put credentials in endpoint URLs, command arguments, source control, or delivery reports.

```bash
export PUBLISHER_S3_ENDPOINT=http://localhost:8333
export PUBLISHER_KAFKA_BOOTSTRAP=localhost:9092
# Set PUBLISHER_KAFKA_CONFIG to a private external JSON file when needed.

local/tmp/publisher-env/bin/python -m data.data_source.publisher publish \
  local/runs/small --state local/runs/small-publisher --mode bulk
```

The bucket and topic must already exist. The publisher does not create infrastructure, delete destination objects, or reset Kafka. The platform holds one active dataset. Use a destination prepared for that dataset, because unrelated old objects and messages remain until an operator removes them.

Replay uses one simulated clock for daily files and cash transactions:

```bash
local/tmp/publisher-env/bin/python -m data.data_source.publisher publish \
  local/runs/small --state local/runs/small-replay \
  --mode replay --speed 86400 --daily-release end
```

A speed of `86400` maps one simulated day to one wall-clock second. Initial files have no clock wait. By default, daily extracts become due at the end of their represented UTC date. `--daily-release start` makes them due at the start instead. This setting controls replay timing, not a source delivery service guarantee. FINWIRE comes before other extracts within its date. Stream messages become due at their saved event times. Bulk mode uses the same stable order without clock waits.

The first pending daily item starts the replay clock. A resumed command starts a new clock segment at its next pending item instead of waiting through acknowledged history. Reports retain segment counts and the latest simulated and wall-clock anchors. Event timestamps and source order do not change. Kafka record timestamps use actual delivery wall time, not accelerated simulated time. Compare event and delivery times only with the replay mapping and segment boundaries.

## Source Protection

Preflight finishes before either network client opens. It checks the following conditions:

- `manifest.json` has `status: validated`, a SHA-256 `dataset_id`, the four inclusive run dates, and a `files` list. Each entry has `path`, `rows`, `bytes`, and `sha256`. `check` and `publish` accept `status: generated-unvalidated` only with the explicit `--allow-unvalidated` flag. The flag does not bypass any publisher preflight check. Unknown generator metadata does not enter the delivery path.
- Manifest and actual source coverage match an independently derived cadence, including empty files, monthly prospect snapshots, quarterly FINWIRE releases, and every calendar date. The supported date range is 1902 to 2100.
- Every source file matches its saved byte count and SHA-256. Offline contents stay unchanged, including intentional source-recording data errors. The generator validator owns offline business and field validation.
- Initial cash transaction IDs start at one. Saved stream IDs continue the contiguous sequence. Stream timestamps match their file date, and records follow event-time and ID order.
- Saved stream values use compact UTF-8 JSON in this exact order: `ct_id`, `ct_ca_id`, `ct_dts`, `ct_amt`, `ct_name`, `schema_version`. Amounts remain decimal text. Kafka values preserve the saved bytes, except the JSON Lines separator LF. Kafka keys use the decimal ASCII account ID.
- Source paths contain no traversal, links, or special files. Reopening a source checks each path component without following links.

Before each delivery, the publisher copies one source file to a private temporary file and checks its hash again. If an edit after preflight changes the selected bytes, the hash check stops delivery. An edit during upload cannot change the private snapshot. The publisher never writes to the generator dataset. Only the `offline/` prefix is removed for S3 keys; no dataset prefix or control file enters `upstream`. Object metadata carries the source SHA-256, but an ETag is not treated as a content hash.

## Acknowledgments and Resume

`--state` names a private local directory outside the dataset. It holds `delivery.sqlite`, a process lock, and `report.json`. SQLite records each object outcome and one cursor per saved stream file. The journal binds acknowledgments to dataset identity, exact source inventory, S3 endpoint and bucket, and Kafka bootstrap addresses and topic. A different source or destination requires separate state. Do not move or reset the destination while reusing an acknowledged checkpoint.

Kafka batches contain at most 256 messages and 8 MiB of payload. A WAL transaction identifies the batch before sends begin. The cursor advances only after every delivery callback succeeds. Checkpoints use full SQLite synchronization per batch, not per message. A failed callback or a lost process leaves the whole batch uncertain, even if some messages succeeded or never left the client. The uncertain range is a conservative upper bound.

| Outcome | Next command behavior | Meaning |
| --- | --- | --- |
| Acknowledged object or stream batch | Skip its saved identity | The client received success and committed a local checkpoint |
| Local source or setup failure before sending | Stop with a fixed safe error | No new delivery began for that item |
| Send exception, missing acknowledgment, or interrupted in-flight checkpoint | Stop and require review | The remote service may have accepted some or all data |

If an uncertain delivery needs a resend, review `report.json` and repeat the same command with `--retry-uncertain`. The retry keeps object keys, source hashes, cash transaction IDs, account keys, and payloads. It can create another object version or duplicate Kafka messages. Reports count explicit uncertain retries and possible duplicate message upper bounds. Kafka producer idempotence limits retries within its producer session; it does not make a new session or the S3 and Kafka combination exactly once.

The report includes acknowledged object and message counts, acknowledged bytes, completed stream files, unresolved identities, clock segment data, and safe error codes. It records the generator status and the `allow_unvalidated` choice for each clock segment. `unvalidated_delivery_segments` counts earlier segments that accepted an unvalidated dataset, even if a later command uses a validated manifest. The JSON report lists at most 100 unresolved files; SQLite retains all file checkpoints. It does not store credentials, connection strings, full cash transaction payloads, or hidden state. A hard process or disk failure can leave the JSON report stale. The next command recovers the durable SQLite state before it permits another send.

## Resource Bounds and Code Map

The publisher processes files and dates in order. It opens one source and one private snapshot at a time. The snapshot needs temporary disk equal to the largest pending file. Copy buffers use 1 MiB. A saved message can use at most 64 KiB. Metadata input is capped at 32 MiB, and parsed metadata uses memory proportional to that bounded inventory. Kafka queues use the same 256-message and 8 MiB bounds as publisher batches. S3 multipart transfers use one worker and 8 MiB parts. SQLite uses a 4 MiB page-cache target and disk-backed temporary tables. These are component limits, not a measured process RSS guarantee.

| Module | Responsibility |
| --- | --- |
| `cli.py`, `__main__.py` | Commands, external settings, and safe terminal errors |
| `dataset.py` | Manifest, independent cadence, hashes, ordered cash messages, and verified snapshots |
| `clock.py` | One monotonic simulated replay clock |
| `journal.py` | Private SQLite checkpoints, process lock, clock segments, and safe reports |
| `delivery.py` | Preflight, initial files, daily files, bounded message batches, and resume |
| `transports.py` | Optional S3 and Kafka clients with bounded queues and safe errors |
| `errors.py` | Local failures and uncertain remote outcomes |
| `tests/` | Mock integration, corruption, replay, retry, redaction, CLI help, and a small real generator result |

Run the component tests in an environment that includes the generator test dependencies:

```bash
.venv/bin/python -m pytest data/data_source/publisher/tests -q
```

The tests use controlled S3 and Kafka adapters. A deployment check sends a small saved dataset to the chosen services, verifies S3 object bytes and keys, and consumes the raw topic to compare message keys, payload bytes, order within each account, and record timestamps. Keep the resulting delivery report outside the generator dataset.
