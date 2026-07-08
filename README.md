# kafka-data-keep
DR utility for preserving kafka topics and consumer groups data in S3, with the ability to restore.

# Performance

Benchmarked on an MSK 3.8.x cluster with 9 `kafka.m7g.2xlarge` nodes.

## Backup

**Topics:** 179 topics (~2400 partitions) backed up in **3h 45min**, producing **~1 TB** of Avro-compressed files on S3 with up to 10 parallel replicas. Sustained throughput: **~78 MB/s**.

**Consumer groups:** ~500 groups backed up in **~500ms**.

## Restore

**Topics:** 177 topics (~2300 partitions) restored from 600 GB of compressed Avro files in **1h** (2 large topics excluded). Sustained throughput: **~166 MB/s**. Peak record throughput reached **1.84 million records/s** using a plan topic with 50 partitions and up to 30 restore replicas. Kafka broker CPU peaked at 40%.

**Consumer groups:** Restore runs alongside the topics restore and progresses with it. It typically completes within ~1 minute after the topics restore finishes. See [Operations](#operations) for details.

# Topics backup
## Approach

The application functions as a Kafka consumer that backs up data to S3. It uses a local buffering strategy to ensure data integrity and efficient uploads.

1.  **Consumption**: Each partition data is written to a separate file.
2.  **Local Buffering**: Messages are encoded (using Avro) and written to local files on disk.
    This acts as a buffer before uploading to S3 and is more memory efficient than "streaming" to S3, as the S3 SDK implementation uses a minimum 5MB buffer per file.
3.  **Rotation & Upload**:
    - The application monitors the size of the local file.
    - Once a file reaches a configured minimum size (`MinFileSize`), the file is uploaded to S3, the Kafka offsets are committed and the local file is deleted
4.  **Idempotence**: The file naming convention (based on offsets) ensures that if the application restarts, it can resume without duplicating data or missing messages, as the offset commit happens only after a successful upload.

## Known Limitations

- **Duplicate Header Keys**: If a Kafka record contains multiple headers with the same key, only the last header value is kept. All previous values for that key are dropped during backup.


## Persistence

The application organizes files locally and in S3 using a structured hierarchy and a deterministic naming convention.

### Directory Structure

Files are stored in the following directory structure:

```
<RootPath>/<S3Prefix>/<topic>/<partition>/
```

-   `RootPath`: The local base directory for buffering files.
-   `S3Prefix`: A prefix used for S3 keys (and mirrored locally).
-   `topic`: The name of the Kafka topic.
-   `partition`: The partition ID.

### File Naming Convention

Files are named using the following format:

```
<topic>-<partition>-<padded_offset>.avro
```

-   `topic`: The name of the Kafka topic.
-   `partition`: The partition ID.
-   `padded_offset`: The Kafka offset of the **first message** in the file, zero-padded to 19 digits.

**Example:**
For topic `my-topic`, partition `0`, and starting offset `12345`:

```
my-topic-0-0000000000000012345.avro
```

The 19-digit zero-padding ensures that files are sorted chronologically when listed alphabetically, which is essential for having the messages sorted on restore.

Since Kafka offsets are only committed after a successful S3 upload, this file naming convention makes the backup process idempotent in most scenarios.
However, overlapping files can occasionally be generated, such as when a partition's starting offset advances due to log retention policies.
The restore process handles these edge cases by automatically detecting and skipping duplicate records. For more details, see [Deduplication](#deduplication).

### Data Durability
Any records not yet uploaded to S3 are stored durably in the configured `WorkingDir` as local files. It is mandatory for this to be a durable storage that pesists between app restarts. The application deliberately does **not** flush local files to S3 on crash or exit — this avoids triggering an unnecessary upload when individual brokers are temporarily unavailable (e.g. during a rolling upgrade) while the cluster is otherwise functional.

Instead, the cluster's availability is checked on startup via a connection ping. If the ping fails, the entire cluster is considered unavailable and the application uploads all local `.avro` files to S3 before exiting. This ensures all the buffered data reaches S3 before a full-cluster restore is initiated. The expected sequence for a full-cluster outage is:

1. The app crashes because Kafka becomes fully unavailable — buffered data remains on disk.
2. The app is restarted (e.g. by Kubernetes).
3. On startup, the Kafka ping fails — the app uploads the local files to S3 and exits.

### Idle Partitions
To optimize memory usage, the application automatically closes local files associated with partitions that become idle (i.e., do not receive new data for a configurable duration).
- **Threshold Control**: This behavior is governed by the `partition-idle-threshold` parameter.
- **Memory Optimization**: Keeping this threshold to a low value significantly improves memory usage by limiting the number of concurrently open file descriptors and buffered writers. By default this is set to 5s.


## Configuration

The `topics-backup` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag | Environment Variable | Default             | Description |
| :--- | :--- |:--------------------| :--- |
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092`    | Kafka brokers (comma separated) |
| `-brokersDNSSrv` | `KAFKA_BROKERS_DNS_SRV` |                     | DNS SRV record with the kafka seed brokers |
| `-kafka-mtls-auth` | `KAFKA_MTLS_AUTH` | `false`             | Kafka cluster uses mTLS authentication |
| `-kafka-mtls-ca-cert-path` | `KAFKA_MTLS_CA_CERT_PATH` | `/certs/ca.crt`     | The path of the file containing the CA cert |
| `-kafka-mtls-client-cert-path` | `KAFKA_MTLS_CLIENT_CERT_PATH` | `/certs/tls.crt`    | The path of the file containing the client cert |
| `-kafka-mtls-client-key-path` | `KAFKA_MTLS_CLIENT_KEY_PATH` | `/certs/tls.key`    | The path of the file containing the client private key |
| `-kgo-log-level` | `KGO_LOG_LEVEL` | `INFO`              | The log level for the franz-go library |
| `-topics-regex` | `KAFKA_TOPICS_REGEX` | `.*`                | List of kafka topics regex to consume (comma separated) |
| `-exclude-topics-regex` | `KAFKA_EXCLUDE_TOPICS_REGEX` |                     | List of kafka topics regex to exclude from consuming (comma separated) |
| `-group-id` | `KAFKA_GROUP_ID` | `kafka-data-keep`   | Kafka consumer group ID |
| `-s3-bucket` | `S3_BUCKET` |                     | S3 bucket name where to store the backups |
| `-s3-prefix` | `S3_PREFIX` |                     | The prefix to use for the backup files in S3 |
| `-min-file-size` | `MIN_FILE_SIZE` | `5242880` (5MB)     | The minimum file size in bytes for each partition backup file |
| `-partition-idle-threshold` | `PARTITION_IDLE_THRESHOLD` | `5s`                | The threshold after which a partition will be considered idle for not consuming any new records (duration, e.g. `30s`, `5m`) |
| `-working-dir` | `WORKING_DIR` | `kafka-backup-data` | Working directory for local files |
| `-s3-endpoint` | `AWS_ENDPOINT_URL` | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region` | `AWS_REGION` | `eu-west-1` | S3 region |
| `-log-level` | `LOG_LEVEL` | `INFO` | The log level to use |
| `-log-format` | `LOG_FORMAT` | `text` | The log format to use (text, json) |
| `-metrics-port` | `METRICS_PORT` | `8081` | The port to use for the metrics server |
| `-enable-pprof` | `ENABLE_PPROF` | `false` | Enable pprof server for profiling |
| `-pprof-port` | `PPROF_PORT` | `6060` | The port to use for the pprof server |
| `-enable-flush-server` | `ENABLE_FLUSH_SERVER` | `false` | Enable HTTP server for flushing partition writers |
| `-flush-server-port` | `FLUSH_SERVER_PORT` | `8082` | The port to use for the flush HTTP server |

## Graceful Shutdown

To ensure optimal performance and operational reliability, the application implements the following signal handling and buffering strategy:

### Shutdown Behavior
Process termination **does not** trigger an automatic flush to S3. This is a deliberate design choice to:
1.  **Maintain Restore Performance**: Respecting `MinFileSize` prevents the creation of small file fragments in S3, which would otherwise degrade restore throughput.
2.  **Minimize Shutdown Latency**: Ensures near-instantaneous process exit by avoiding potentially slow, multi-partition I/O operations during shutdown.

### Manual Flush (SIGUSR1 and HTTP)
To force an immediate persistence of all pending buffers without stopping the service (e.g., before node maintenance or scaling), you can trigger a manual flush in two ways:

**Via Signal:**
```bash
kill -SIGUSR1 <pid>
```

**Via HTTP:**
If configured with `-enable-flush-server` or `ENABLE_FLUSH_SERVER=true` (default: false), send a POST request to the `/__/flush` endpoint on the configured flush server port (`8082` by default). The server binds to `127.0.0.1`:
```bash
curl -X POST http://127.0.0.1:8082/__/flush
```

Both methods will immediately flush all active partition writers to S3.


### Usage Example

```console
./kafka-data-keep topics-backup \
  -brokers "kafka:9092" \
  -topics-regex "my-topic.*" \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1" \
  -partition-idle-threshold "5s"
```

# Topics Plan Restore

Prepares the restore plan by reading the backup files on the S3 bucket and, for each file, it will create a record in a Kafka topic.
We are using this intermediary phase, before restore, to leverage the kafka consumer group functionality to achieve parallelism per partition safely at restore time and to keep track of the restore process.

The topics will be processed in the order specified in the `restore-topics-regex` list. If multiple topics match a particular regex, they'll be processed in alphabetical order.

In case something fails during the execution, it will resume the listing by reading the last messages produced on the kafka topic.

### Deferring large topics

When `-process-large-topics-last` is set, topics whose total S3 backup size exceeds `-large-topic-threshold-mb` are moved to the end of the plan. Topics below the threshold are planned first (preserving their original order), followed by the large ones (also preserving their relative order). This is useful to avoid a single large topic blocking the restoration of many smaller ones.

### Plan record headers

Each record produced into the plan topic carries two headers that expose per-partition progress:

| Header | Description |
| :--- | :--- |
| `plan-restore.file-index` | 1-based index of this file within its partition (absolute across resumes) |
| `plan-restore.total-files` | Total number of backup files for this partition |

The `topics-restore` command reads these headers and exposes them as Prometheus gauges (see [Restore metrics](#restore-metrics)).

## Configuration

The `plan-restore` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv` | `KAFKA_BROKERS_DNS_SRV` | | DNS SRV record with the kafka seed brokers |
| `-kafka-mtls-auth` | `KAFKA_MTLS_AUTH` | `false` | Kafka cluster uses mTLS authentication |
| `-kafka-mtls-ca-cert-path` | `KAFKA_MTLS_CA_CERT_PATH` | `/certs/ca.crt` | The path of the file containing the CA cert |
| `-kafka-mtls-client-cert-path` | `KAFKA_MTLS_CLIENT_CERT_PATH` | `/certs/tls.crt` | The path of the file containing the client cert |
| `-kafka-mtls-client-key-path` | `KAFKA_MTLS_CLIENT_KEY_PATH` | `/certs/tls.key` | The path of the file containing the client private key |
| `-kgo-log-level` | `KGO_LOG_LEVEL` | `INFO` | The log level for the franz-go library |
| `-restore-topics-regex` | `RESTORE_TOPICS_REGEX` | `.*` | List of regex to match topics to restore (comma separated). The topics will be restored in the order specified in this list. |
| `-exclude-topics-regex` | `EXCLUDE_TOPICS_REGEX` | | List of regex to exclude topics from restore (comma separated) |
| `-plan-topic` | `PLAN_TOPIC` | `pubsub.plan-topic-restore` | Kafka topic to send the restore plan to |
| `-s3-bucket` | `S3_BUCKET` | | S3 bucket name where the backup files are stored |
| `-s3-prefix` | `S3_PREFIX` | `msk-backup` | The prefix for the backup files in S3 |
| `-process-large-topics-last` | `PROCESS_LARGE_TOPICS_LAST` | `false` | When set, topics whose total S3 size exceeds `-large-topic-threshold-mb` are planned after all smaller topics |
| `-large-topic-threshold-mb` | `LARGE_TOPIC_THRESHOLD_MB` | `92160` (90 GB) | Size threshold in megabytes above which a topic is considered large (used with `-process-large-topics-last`) |
| `-s3-endpoint` | `AWS_ENDPOINT_URL` | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region` | `AWS_REGION` | `eu-west-1` | S3 region |
| `-log-level` | `LOG_LEVEL` | `INFO` | The log level to use |
| `-log-format` | `LOG_FORMAT` | `text` | The log format to use (text, json) |
| `-metrics-port` | `METRICS_PORT` | `8081` | The port to use for the metrics server |
| `-enable-pprof` | `ENABLE_PPROF` | `false` | Enable pprof server for profiling |
| `-pprof-port` | `PPROF_PORT` | `6060` | The port to use for the pprof server |

## Usage Example

```console
./kafka-data-keep topics-plan-restore \
  -brokers "kafka:9092" \
  -restore-topics-regex "domain1.*, domain2.*" \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1"
```

# Topics restore

The `topics-restore` command consumes restore plan records from the plan topic (created by `plan-restore`) and restores the data from S3 backup files back to Kafka topics.

## Key Features

### Data
It restores the records in their original partition, keeping the order of the messages.
It keeps all the data from the original record: key, value, timestamp, headers (note: duplicate header keys are dropped during backup, see Topics backup limitations).
In addition, it adds the `restore.source-offset` header to each restored message pointing to the original Kafka offset in the source topic. Uses this for resuming and deduplication.

### Deduplication

The restore command automatically handles duplicate S3 backup files with different offsets. When multiple files exist for the same partition with overlapping data (e.g., due to backup process failures and retries), the restore process will:
- Detect duplicate records based on their original Kafka offsets (stored in the `restore.source-offset` header)
- Skip records that have already been restored based on their original offset to prevent duplicates in the target topic.

This ensures that even if S3 contains redundant backup files, the restored Kafka topic will contain each message exactly once.

### Resuming

The restore process supports resuming from where it left off if interrupted. Since the restore command uses a Kafka consumer group to read from the plan topic, it automatically tracks progress via committed offsets. If the restore process is stopped and restarted, it will continue from the last committed offset, ensuring no data is lost or duplicated during restoration.
In addition, to avoid duplicate records due to redeliveries from the plan topic, upon resuming, it will read the last restored message in each partition to determine the last restored offset from the "restore.source-offset" header.

### Parallelism
It supports launching as many instances as the number of partitions in the plan topic.
Each instance will consume a single partition from the plan topic and restore its data from S3.
In the plan topic, the partitioning is done based on the source topic name and partition;
this ensures that all the files holding the data for a topic's partition will be restored in order by the same instance.

### Restore metrics

The restore command exposes per-topic/per-partition progress gauges on the metrics endpoint (`/__/metrics`):

| Metric | Labels | Description |
| :--- | :--- | :--- |
| `kafka_data_keep_restore_partition_total_files` | `topic`, `partition` | Total number of backup files for this partition, as written into the plan |
| `kafka_data_keep_restore_partition_file_index` | `topic`, `partition` | 1-based index of the backup file currently being processed |

These are derived from the `plan-restore.file-index` / `plan-restore.total-files` headers set by `topics-plan-restore`. If a plan was produced by an older version of the planner (without those headers), the gauges are simply not emitted for that run.

## Configuration

The `topics-restore` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag                    | Environment Variable         | Default | Description |
|:------------------------|:-----------------------------| :--- | :--- |
| `-brokers`              | `KAFKA_BROKERS`              | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv`        | `KAFKA_BROKERS_DNS_SRV`      | | DNS SRV record with the kafka seed brokers |
| `-kafka-mtls-auth`      | `KAFKA_MTLS_AUTH`            | `false` | Kafka cluster uses mTLS authentication |
| `-kafka-mtls-ca-cert-path` | `KAFKA_MTLS_CA_CERT_PATH` | `/certs/ca.crt` | The path of the file containing the CA cert |
| `-kafka-mtls-client-cert-path` | `KAFKA_MTLS_CLIENT_CERT_PATH` | `/certs/tls.crt` | The path of the file containing the client cert |
| `-kafka-mtls-client-key-path` | `KAFKA_MTLS_CLIENT_KEY_PATH` | `/certs/tls.key` | The path of the file containing the client private key |
| `-kgo-log-level` | `KGO_LOG_LEVEL` | `INFO` | The log level for the franz-go library |
| `-plan-topic`           | `KAFKA_PLAN_TOPIC`           | `pubsub.plan-topic-restore` | Kafka topic to consume the plan from |
| `-restore-topic-prefix` | `KAFKA_RESTORE_TOPIC_PREFIX` | `pubsub.restore-test.` | Prefix to add to the restored topics |
| `-exclude-topics-regexes` | `EXCLUDE_TOPICS_REGEXES` | | List of regular expressions to exclude topics from restore (comma separated) |
| `-group-id`             | `KAFKA_GROUP_ID`             | `pubsub.msk-data-keep-restore` | Kafka consumer group ID |
| `-s3-bucket`            | `S3_BUCKET`                  | | S3 bucket name where the backups are stored |
| `-s3-endpoint`          | `AWS_ENDPOINT_URL`           | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region`            | `AWS_REGION`                 | `eu-west-1` | S3 region |
| `-log-level` | `LOG_LEVEL` | `INFO` | The log level to use |
| `-log-format` | `LOG_FORMAT` | `text` | The log format to use (text, json) |
| `-metrics-port` | `METRICS_PORT` | `8081` | The port to use for the metrics server |
| `-enable-pprof` | `ENABLE_PPROF` | `false` | Enable pprof server for profiling |
| `-pprof-port` | `PPROF_PORT` | `6060` | The port to use for the pprof server |

## Usage Example

```console
./kafka-data-keep topics-restore \
  -brokers "kafka:9092" \
  -plan-topic "pubsub.plan-topic-restore" \
  -restore-topic-prefix "pubsub.restored." \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1"
```

# Consumer Groups Backup

## Approach

The application periodically snapshots all Kafka consumer group offsets and uploads them to S3 as a single Avro file.

1.  **Discovery**: Lists all consumer groups on the cluster using the Kafka admin API.
2.  **Offset Fetching**: For each consumer group, fetches the committed offsets for all topic partitions.
3.  **Encoding**: The offsets are encoded using Avro and written to a temporary local file.
4.  **Upload**: The file is uploaded to a configured S3 location, overwriting the previous snapshot.
5.  **Scheduling**: The process repeats at a configurable interval (`run-interval`).

## Data Structure

Each Avro record represents a single consumer group and contains:

-   `group_id`: The consumer group ID.
-   `topics`: An array of topics, each containing:
    -   `topic`: The topic name.
    -   `partitions`: An array of partition offsets, each containing:
        -   `partition`: The partition ID.
        -   `offset`: The committed offset.
        -   `leader_epoch`: The leader epoch (optional).
        -   `metadata`: The offset metadata (optional).

## Configuration

The `consumer-groups-backup` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv` | `KAFKA_BROKERS_DNS_SRV` | | DNS SRV record with the kafka seed brokers |
| `-kafka-mtls-auth` | `KAFKA_MTLS_AUTH` | `false` | Kafka cluster uses mTLS authentication |
| `-kafka-mtls-ca-cert-path` | `KAFKA_MTLS_CA_CERT_PATH` | `/certs/ca.crt` | The path of the file containing the CA cert |
| `-kafka-mtls-client-cert-path` | `KAFKA_MTLS_CLIENT_CERT_PATH` | `/certs/tls.crt` | The path of the file containing the client cert |
| `-kafka-mtls-client-key-path` | `KAFKA_MTLS_CLIENT_KEY_PATH` | `/certs/tls.key` | The path of the file containing the client private key |
| `-kgo-log-level` | `KGO_LOG_LEVEL` | `INFO` | The log level for the franz-go library |
| `-s3-bucket` | `S3_BUCKET` | | S3 bucket name where to store the backups |
| `-s3-location` | `S3_LOCATION` | | The S3 location (full path key) to use for the backup file |
| `-run-interval` | `RUN_INTERVAL` | `1m` | Interval between backups (duration, e.g. `30s`, `5m`) |
| `-s3-endpoint` | `AWS_ENDPOINT_URL` | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region` | `AWS_REGION` | `eu-west-1` | S3 region |
| `-log-level` | `LOG_LEVEL` | `INFO` | The log level to use |
| `-log-format` | `LOG_FORMAT` | `text` | The log format to use (text, json) |
| `-metrics-port` | `METRICS_PORT` | `8081` | The port to use for the metrics server |
| `-enable-pprof` | `ENABLE_PPROF` | `false` | Enable pprof server for profiling |
| `-pprof-port` | `PPROF_PORT` | `6060` | The port to use for the pprof server |

### Usage Example

```console
./kafka-data-keep consumer-groups-backup \
  -brokers "kafka:9092" \
  -s3-bucket "my-backup-bucket" \
  -s3-location "backups/consumer-groups/offsets.avro" \
  -run-interval "5m" \
  -s3-region "us-east-1"
```

# Consumer Groups Restore

## Approach

The application restores consumer group offsets from an S3 Avro backup file (created by `consumer-groups-backup`) into a Kafka cluster where topics have been restored using the `topics-restore` command.

1.  **Download & Decode**: Downloads the consumer group offsets Avro file from S3 and decodes all consumer group offset records.
2.  **Filtering**: Filters consumer groups by the include regular expressions, removes groups that already have offsets committed in the cluster, and removes topics that don't exist in the cluster.
3.  **Offset Resolution Loop**: On each iteration:
    -   For each topic, reads the latest record on each partition using the `restore.source-offset` header.
    -   For each consumer group offset, if the source offset in the latest record is greater or equal to the backed-up offset, it computes the new offset and walks forward from that position to find the exact matching record.
    -   Commits the resolved offset for the consumer group.
4.  **Completion**: Exits when all consumer group offsets have been restored.

## Configuration

The `consumer-groups-restore` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv` | `KAFKA_BROKERS_DNS_SRV` | | DNS SRV record with the kafka seed brokers |
| `-kafka-mtls-auth` | `KAFKA_MTLS_AUTH` | `false` | Kafka cluster uses mTLS authentication |
| `-kafka-mtls-ca-cert-path` | `KAFKA_MTLS_CA_CERT_PATH` | `/certs/ca.crt` | The path of the file containing the CA cert |
| `-kafka-mtls-client-cert-path` | `KAFKA_MTLS_CLIENT_CERT_PATH` | `/certs/tls.crt` | The path of the file containing the client cert |
| `-kafka-mtls-client-key-path` | `KAFKA_MTLS_CLIENT_KEY_PATH` | `/certs/tls.key` | The path of the file containing the client private key |
| `-kgo-log-level` | `KGO_LOG_LEVEL` | `INFO` | The log level for the franz-go library |
| `-s3-bucket` | `S3_BUCKET` | | S3 bucket name where the consumer groups backup is stored |
| `-s3-location` | `S3_LOCATION` | | The S3 location (full path key) of the consumer groups backup file |
| `-restore-groups-prefix` | `RESTORE_GROUPS_PREFIX` | | Prefix to add to the restored consumer group names |
| `-restore-topics-prefix` | `RESTORE_TOPICS_PREFIX` | | Prefix used on the restored topic names |
| `-include-groups-regexes` | `INCLUDE_GROUPS_REGEXES` | `.*` | List of regular expressions to match consumer groups to restore (comma separated) |
| `-exclude-groups-regexes` | `EXCLUDE_GROUPS_REGEXES` | | List of regular expressions to exclude consumer groups from restore (comma separated) |
| `-exclude-topics-regexes` | `EXCLUDE_TOPICS_REGEXES` | | List of regular expressions to exclude topics from restore (comma separated) |
| `-loop-interval` | `LOOP_INTERVAL` | `1m` | Duration between consumer group restore iterations (e.g. `30s`, `5m`) |
| `-s3-endpoint` | `AWS_ENDPOINT_URL` | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region` | `AWS_REGION` | `eu-west-1` | S3 region |
| `-log-level` | `LOG_LEVEL` | `INFO` | The log level to use |
| `-log-format` | `LOG_FORMAT` | `text` | The log format to use (text, json) |
| `-metrics-port` | `METRICS_PORT` | `8081` | The port to use for the metrics server |
| `-enable-pprof` | `ENABLE_PPROF` | `false` | Enable pprof server for profiling |
| `-pprof-port` | `PPROF_PORT` | `6060` | The port to use for the pprof server |

### Usage Example

```console
./kafka-data-keep consumer-groups-restore \
  -brokers "kafka:9092" \
  -s3-bucket "my-backup-bucket" \
  -s3-location "backups/consumer-groups/offsets.avro" \
  -restore-groups-prefix "restored." \
  -restore-topics-prefix "restored." \
  -include-groups-regexes "my-group.*,other-group.*" \
  -exclude-groups-regexes "internal-.*" \
  -loop-interval "1m" \
  -s3-region "us-east-1"
```

# HTTP Endpoints

All commands expose an HTTP server on the port specified by `--metrics-port` or `METRICS_PORT` environment variable (default: `8081`).
The following endpoints are available:
- `/__/metrics`: Prometheus metrics

If `--enable-pprof` or `ENABLE_PPROF` is set to `true`, a separate HTTP server is started on localhost using the port specified by `--pprof-port` or `PPROF_PORT` (default: `6060`).
The following endpoints are available on this server:
- `/debug/pprof/`: pprof index
- `/debug/pprof/cmdline`: pprof cmdline
- `/debug/pprof/profile`: pprof profile
- `/debug/pprof/symbol`: pprof symbol
- `/debug/pprof/trace`: pprof trace

# Operations

## Deployment
Kubernetes manifests are provided through kustomize at ./deploy/kustomize

## Backup
Run the topics backup and consumer group backup as two separate, continuously running processes in parallel.

In Kubernetes:
- **Consumer group backup**: deploy as a Deployment with exactly 1 replica.
- **Topics backup**: deploy as a StatefulSet for automatic PVC provisioning for durable storage. Scale replicas based on your cluster size. More replicas speed up the initial backup. Once the initial backup is complete, tune the replica count to match cluster load — for reference, 2 replicas handles a cluster with 300 topics and 11k total partitions (including replicas).
## Restore
### Considerations
Within a single pipeline, ordering can be controlled via configuration:
1. **Large vs. normal topics** — use the `-process-large-topics-last` flag on `topics-plan-restore` to defer large topics automatically, so they don't block restoration of smaller ones.
2. **High-priority vs. low-priority topics** — list high-priority topics first in the `-restore-topics-regex` parameter (e.g. `high-prio1,high-prio2,.*`) so they are planned and restored before others.

The progress of restore for topics can be monitored through the [exposed metrics](#restore-metrics).
### Steps

1. Provision the new Kafka cluster.
2. Create the plan-restore topic(s). Use 20–50 partitions to maximise parallelism during topics restore.
3. Create the target topics with the **same partition count** as the source.
4. Run `topics-plan-restore` (one per plan-restore topic). These are short-lived Kubernetes Jobs.
5. Run `topics-restore` (one Deployment per plan-restore topic). Scale replicas to match the plan-restore topic's partition count. Monitor consumer group progress to determine completion.
6. Run `consumer-groups-restore`. Can run in parallel with step 5. Runs as a Kubernetes Job and exits when all consumer groups are restored.

> **Note:** Consumer group restore lags behind topic restore because it retries on a timer interval.

### Tips

1. Use Terraform with the [Kafka provider](https://registry.terraform.io/providers/Mongey/kafka/latest/docs) to manage topic definitions. This way, step 3 is just pointing the module at the new cluster and running `terraform apply`.
