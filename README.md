# kafka-data-keep
DR utility for preserving kafka topics and consumer groups data in S3, with the ability to restore.

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

## File Naming and Directory Structure

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

## Configuration

The `topics-backup` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv` | `KAFKA_BROKERS_DNS_SRV` | | DNS SRV record with the kafka seed brokers |
| `-topics-regex` | `KAFKA_TOPICS_REGEX` | `.*` | List of kafka topics regex to consume (comma separated) |
| `-exclude-topics-regex` | `KAFKA_EXCLUDE_TOPICS_REGEX` | | List of kafka topics regex to exclude from consuming (comma separated) |
| `-group-id` | `KAFKA_GROUP_ID` | `kafka-data-keep` | Kafka consumer group ID |
| `-s3-bucket` | `S3_BUCKET` | | S3 bucket name where to store the backups |
| `-s3-prefix` | `S3_PREFIX` | | The prefix to use for the backup files in S3 |
| `-min-file-size` | `MIN_FILE_SIZE` | `5242880` (5MB) | The minimum file size in bytes for each partition backup file |
| `-working-dir` | `WORKING_DIR` | `kafka-backup-data` | Working directory for local files |
| `-s3-endpoint` | `AWS_ENDPOINT_URL` | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region` | `AWS_REGION` | `eu-west-1` | S3 region |

### Usage Example

```console
./kafka-data-keep topics-backup \
  -brokers "kafka:9092" \
  -topics-regex "my-topic.*" \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1"
```

# Topics Plan Restore

Prepares the restore plan by reading the backup files on the S3 bucket and, for each file, it will create a record in a Kafka topic. 
We are using this intermediary phase, before restore, to leverage the kafka consumer group functionality to achieve parallelism per partition safely at restore time and to keep track of the restore process.

The topics will be processed in the order specified in the `restore-topics-regex` list. If multiple topics match a particular regex, they'll be processed in alphabetical order. 

In case something fails during the execution, it will resume the listing by reading the last messages produced on the kafka topic.

## Configuration

The `plan-restore` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv` | `KAFKA_BROKERS_DNS_SRV` | | DNS SRV record with the kafka seed brokers |
| `-restore-topics-regex` | `RESTORE_TOPICS_REGEX` | `.*` | List of regex to match topics to restore (comma separated). The topics will be restored in the order specified in this list. |
| `-exclude-topics-regex` | `EXCLUDE_TOPICS_REGEX` | | List of regex to exclude topics from restore (comma separated) |
| `-plan-topic` | `PLAN_TOPIC` | `pubsub.plan-topic-restore` | Kafka topic to send the restore plan to |
| `-s3-bucket` | `S3_BUCKET` | | S3 bucket name where the backup files are stored |
| `-s3-prefix` | `S3_PREFIX` | `msk-backup` | The prefix for the backup files in S3 |
| `-s3-endpoint` | `AWS_ENDPOINT_URL` | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region` | `AWS_REGION` | `eu-west-1` | S3 region |

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
It keeps all the data from the original record: key, value, timestamp, headers.
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

## Configuration

The `topics-restore` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

| Flag                    | Environment Variable         | Default | Description |
|:------------------------|:-----------------------------| :--- | :--- |
| `-brokers`              | `KAFKA_BROKERS`              | `localhost:9092` | Kafka brokers (comma separated) |
| `-brokersDNSSrv`        | `KAFKA_BROKERS_DNS_SRV`      | | DNS SRV record with the kafka seed brokers |
| `-plan-topic`           | `KAFKA_PLAN_TOPIC`           | `pubsub.plan-topic-restore` | Kafka topic to consume the plan from |
| `-restore-topic-prefix` | `KAFKA_RESTORE_TOPIC_PREFIX` | `pubsub.restore-test.` | Prefix to add to the restored topics |
| `-group-id`             | `KAFKA_GROUP_ID`             | `pubsub.msk-data-keep-restore` | Kafka consumer group ID |
| `-s3-bucket`            | `S3_BUCKET`                  | | S3 bucket name where the backups are stored |
| `-s3-endpoint`          | `AWS_ENDPOINT_URL`           | | S3 endpoint URL (for LocalStack or custom S3-compatible storage) |
| `-s3-region`            | `AWS_REGION`                 | `eu-west-1` | S3 region |

## Usage Example

```console
./kafka-data-keep topics-restore \
  -brokers "kafka:9092" \
  -plan-topic "pubsub.plan-topic-restore" \
  -restore-topic-prefix "pubsub.restored." \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1"
```
