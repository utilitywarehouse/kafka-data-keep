# kafka-data-keep
DR utility for preserving kafka topics data in S3, with the ability to restore

# Backup
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

The `backup` subcommand supports the following flags and environment variables. Flags take precedence over environment variables.

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

```
./kafka-data-keep backup \
  -brokers "kafka:9092" \
  -topics-regex "my-topic.*" \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1"
```

# Plan Restore

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

```bash
./kafka-data-keep plan-restore \
  -brokers "kafka:9092" \
  -restore-topics-regex "domain1.*, domain2.*" \
  -s3-bucket "my-backup-bucket" \
  -s3-region "us-east-1"
```
