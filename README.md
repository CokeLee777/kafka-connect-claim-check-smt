# Kafka Connect Claim Check SMT
This repository provides an open-source Single Message Transform (SMT) for Kafka Connect that implements the Claim Check pattern. 
It enables efficient handling of large message payloads by storing them in external storage and passing only references through Kafka topics.

## Technology Stack

### Core Technologies

- **Java 17** - Target and source compatibility
- **Kafka Connect API 3.8.1** - Core framework for building connectors and SMTs
- **Gradle 8.11** - Build automation with Shadow plugin for uber JAR packaging

### Runtime Dependencies

- **AWS SDK for Java 2.41.8** - Amazon S3 client for cloud storage integration
- **SLF4J 2.0.17** - Logging facade (provided by Kafka Connect runtime)

### Testing Framework

- **JUnit 5.14.2** - Unit testing framework
- **Mockito 5.21.0** - Mocking framework for unit tests
- **Testcontainers 1.21.4** - Integration testing with LocalStack (S3) and Redis

### Build Plugins

- **Shadow JAR Plugin 9.0.0** - Creates uber JAR with relocated dependencies to avoid classpath conflicts

## Features

### ClaimCheck SMT

The initial SMT included in this toolkit is the **ClaimCheck SMT**. This transform allows you to offload large message
payloads from Kafka topics to external storage (like Amazon S3), replacing them with a "claim check" (a reference to the
original data). This helps in reducing Kafka message sizes, improving throughput, and lowering storage costs for Kafka
brokers, while still allowing consumers to retrieve the full message content when needed.

**Current Storage Backends:**

* Amazon S3
* File System

**Supported Connectors:**

The ClaimCheck SMT is compatible with **Kafka Connect 2.0+** and works with any **Source Connector** that produces
structured data using Kafka Connect's `Schema` and `Struct` API.

**Tested and Verified:**

*Source Connectors (ClaimCheckSourceTransform):*

| Connector                                 | Tested Version | Status     | Notes                                                                                      |
|-------------------------------------------|----------------|------------|--------------------------------------------------------------------------------------------|
| **Debezium MySQL CDC Source Connector**   | 2.6.2          | ✅ Verified | Tested with Debezium CDC envelope (before/after/source/op) and deeply nested schemas       |
| **Debezium Oracle CDC Source Connector**  | 2.6.2          | ✅ Verified | Tested with Debezium CDC envelope (before/after/source/op) and deeply nested schemas       |
| **Debezium MongoDB CDC Source Connector** | 2.6.2          | ✅ Verified | Tested with Debezium CDC envelope (after/patch/filter/source/op) and deeply nested schemas |
| **Confluent JDBC Source Connector**       | 10.7.6         | ✅ Verified | Tested with complex nested Struct schemas (non-CDC, snapshot-based records)                |

*Sink Connectors (ClaimCheckSinkTransform):*

| Connector                         | Tested Version | Status     | Notes                                                                                                       |
|-----------------------------------|----------------|------------|-------------------------------------------------------------------------------------------------------------|
| **Confluent JDBC Sink Connector** | 10.7.6         | ✅ Verified | Tested with restored Struct records from claim check references, including primitive and nested field types |

> **Note:** If you test this SMT with other connectors, please consider contributing your findings via GitHub issues or
> pull requests!

**Kafka Connect Version Compatibility:**

**Tested Environment:**

- ✅ **Confluent Platform 7.8.0** (includes Apache Kafka 3.6.x) - Verified
- ✅ Built against **Kafka Connect API 3.8.1** for forward compatibility

> **Note:** The SMT is built against Kafka Connect API 3.8.1 but tested on Confluent Platform 7.8.0 (Kafka 3.6.x). The
> backward compatibility of Connect API allows newer builds to work on older runtime versions.

**Technical Requirements:**

The ClaimCheck SMT operates at the **pre-serialization stage** of the Kafka Connect pipeline:

```text
Source Connector → SMT (ClaimCheck) → Converter (JSON/Avro/String) → Kafka Broker
```

```text
Kafka Broker → Converter (JSON/Avro/String) → SMT (ClaimCheck) → Sink Connector
```

#### 🛠 Data Type Support & Converter Compatibility

##### 1. Comprehensive Data Type Support

This SMT fully supports standard Kafka Connect data types in both Schema and Schemaless modes. It seamlessly handles:

- Complex Types: Struct, Map, Array
- Primitive Types: String, Integer, Long, byte[], etc.

##### 2. Choosing the Right Converter

The SMT processes records before they reach the Kafka Connect Converter. Therefore, your value.converter must be compatible with the data type produced by your Source Connector, regardless of this SMT.

- For Structured Data (e.g., Debezium, JDBC): Use AvroConverter, JsonConverter, or ProtobufConverter.
- For Raw Data: Use StringConverter or ByteArrayConverter.

> **⚠️ Important Note on ByteArrayConverter**
> 
> While ByteArrayConverter can handle offloaded records (which become null or default values), it cannot process complex types (like Struct) that are not offloaded.
> 
> - **Do not use** ByteArrayConverter if your Source Connector produces Structs or Maps. 
> - It will fail on any record that skips the claim check process.

#### ⚠️ Metadata Preservation Contract (Important for Debezium Users)

When used with Debezium CDC connectors (e.g., Debezium MySQL), the following behavior is intentional:

1.  **In `ClaimCheckSourceTransform`:**
    Debezium metadata fields (`op`, `ts_ms`, `source`) and CDC envelope structures (`before`, `after`) are temporarily replaced with **default placeholder values** when the payload is offloaded.

2.  **In `ClaimCheckSinkTransform`:**
    These fields are **fully restored** when the record is reconstructed.

**Note:** This temporary replacement is required for the Claim Check pattern to function correctly and preserve schema consistency.

#### Important Implications

- The Source SMT and Sink SMT form a logical pair
- Using only the Source SMT without the Sink SMT will result in metadata loss
- Any modification to the Sink SMT’s restoration logic may break metadata consistency

This design ensures minimal message size in Kafka while preserving the full original record semantics across the pipeline.

#### Configuration

To use the ClaimCheck SMT, you'll need to configure it in your Kafka Connect connector.

##### Source Connector Configuration

*S3 Storage Example:*
```jsonc
{
  "name": "my-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "topic.prefix": "my-prefix-",
    "transforms": "claimcheck",
    "transforms.claimcheck.type": "com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSourceTransform",
    "transforms.claimcheck.threshold.bytes": "1048576",
    "transforms.claimcheck.storage.type": "s3",
    "transforms.claimcheck.storage.s3.bucket.name": "your-s3-bucket-name",
    "transforms.claimcheck.storage.s3.region": "your-aws-region",
    "transforms.claimcheck.storage.s3.path.prefix": "your-s3/prefix/path",
    "transforms.claimcheck.storage.s3.retry.max": "3",
    "transforms.claimcheck.storage.s3.retry.backoff.ms": "300",
    "transforms.claimcheck.storage.s3.retry.max.backoff.ms": "20000"
    // ... other connector configurations
  }
}
```

*File System Storage Example:*
```jsonc
{
  "name": "my-source-connector-fs",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "topic.prefix": "my-prefix-",
    "transforms": "claimcheck",
    "transforms.claimcheck.type": "com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSourceTransform",
    "transforms.claimcheck.threshold.bytes": "1048576",
    "transforms.claimcheck.storage.type": "filesystem",
    "transforms.claimcheck.storage.filesystem.path": "/path/to/your/claim-checks",
    "transforms.claimcheck.storage.filesystem.retry.max": "3",
    "transforms.claimcheck.storage.filesystem.retry.backoff.ms": "300",
    "transforms.claimcheck.storage.filesystem.retry.max.backoff.ms": "20000"
    // ... other connector configurations
  }
}
```

**Important for Distributed Deployments:** When using File System storage in a distributed Kafka Connect cluster with multiple workers:

- Use a **shared network storage** (e.g., NFS, SMB/CIFS, or a distributed file system) mounted at the same path on all worker nodes
- Ensure all Kafka Connect worker processes have **read/write permissions** to the storage path
- Use **absolute paths** to avoid ambiguity across different worker environments
- For production deployments, consider implementing **file system monitoring and alerting** for storage availability
- **Security:** Ensure proper file permissions are set to restrict access to authorized Connect workers only. Consider encryption at rest for sensitive payloads.

##### Sink Connector Configuration

*S3 Storage Example:*
```jsonc
{
  "name": "my-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "my-topic",
    "transforms": "claimcheck",
    "transforms.claimcheck.type": "com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSinkTransform",
    "transforms.claimcheck.storage.type": "s3",
    "transforms.claimcheck.storage.s3.bucket.name": "your-s3-bucket-name",
    "transforms.claimcheck.storage.s3.region": "your-aws-region",
    "transforms.claimcheck.storage.s3.path.prefix": "your-s3/prefix/path",
    "transforms.claimcheck.storage.s3.retry.max": "3",
    "transforms.claimcheck.storage.s3.retry.backoff.ms": "300",
    "transforms.claimcheck.storage.s3.retry.max.backoff.ms": "20000"
    // ... other connector configurations
  }
}
```

*File System Storage Example:*
```jsonc
{
  "name": "my-sink-connector-fs",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "my-topic",
    "transforms": "claimcheck",
    "transforms.claimcheck.type": "com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSinkTransform",
    "transforms.claimcheck.storage.type": "filesystem",
    "transforms.claimcheck.storage.filesystem.path": "/path/to/your/claim-checks",
    "transforms.claimcheck.storage.filesystem.retry.max": "3",
    "transforms.claimcheck.storage.filesystem.retry.backoff.ms": "300",
    "transforms.claimcheck.storage.filesystem.retry.max.backoff.ms": "20000"
    // ... other connector configurations
  }
}
```

**ClaimCheck SMT Configuration Properties:**

*ClaimCheckSourceTransform Properties:*

| Property          | Required | Default         | Description                                                                      |
|-------------------|----------|-----------------|----------------------------------------------------------------------------------|
| `threshold.bytes` | No       | `1048576` (1MB) | Messages larger than this size (in bytes) will be offloaded to external storage. |
| `storage.type`    | Yes      | -               | The type of storage backend to use (e.g., `s3`, `filesystem`).                                 |

*ClaimCheckSinkTransform Properties:*

| Property       | Required | Default | Description                                                                                      |
|----------------|----------|---------|--------------------------------------------------------------------------------------------------|
| `storage.type` | Yes      | -       | The type of storage backend to use (e.g., `s3`, `filesystem`). Must match the source connector's storage type. |

*Common S3 Storage Properties (Both Source and Sink):*

| Property                          | Required | Default          | Description                                                                                                                                                                                          |
|-----------------------------------|----------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `storage.s3.bucket.name`          | Yes      | -                | The name of the S3 bucket to store/retrieve the offloaded messages.                                                                                                                                  |
| `storage.s3.region`               | No       | `ap-northeast-2` | The AWS region of the S3 bucket.                                                                                                                                                                     |
| `storage.s3.path.prefix`          | No       | `claim-checks`   | The prefix (directory path) within the S3 bucket where offloaded messages are stored. Objects are named with a randomly generated UUID. Example: `claim-checks/3f2a1b4e-9c7d-4a2f-8b6e-0d1c2e3f4a5b` |
| `storage.s3.retry.max`            | No       | `3`              | Maximum number of retry attempts for S3 operations (excluding the initial attempt). Set to `0` to disable retries.                                                                                   |
| `storage.s3.retry.backoff.ms`     | No       | `300`            | Initial backoff delay (in milliseconds) before retrying. Used as the base for exponential backoff.                                                                                                   |
| `storage.s3.retry.max.backoff.ms` | No       | `20000`          | Maximum backoff delay (in milliseconds) between retries. Caps the exponential backoff calculation.                                                                                                   |

*Common File System Storage Properties (Both Source and Sink):*

| Property                    | Required | Default          | Description                                                                                                                                |
|-----------------------------|----------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `storage.filesystem.path`   | No       | `claim-checks`   | The directory path for storing claim check files. **Absolute paths are strongly recommended for production deployments**. This can be a local path (single-worker only) or a network-mounted path (e.g., NFS, SMB) for distributed deployments. The path is created if it does not exist with default system permissions. Ensure proper read/write permissions for the Connect worker process. |
| `storage.filesystem.retry.max`            | No       | `3`              | Maximum number of retry attempts for file system operations (excluding the initial attempt). Set to `0` to disable retries.                                                                                   |
| `storage.filesystem.retry.backoff.ms`     | No       | `300`            | Initial backoff delay (in milliseconds) before retrying. Used as the base for exponential backoff.                                                                                                   |
| `storage.filesystem.retry.max.backoff.ms` | No       | `20000`          | Maximum backoff delay (in milliseconds) between retries. Caps the exponential backoff calculation.                                                                                                   |

> **Important:** The Sink connector's storage configuration must match the Source
> connector's configuration to correctly retrieve the offloaded payloads. For example, if using the File System backend, both connectors must point to the same directory path.

#### Usage

Once configured and deployed, the ClaimCheck SMT will automatically intercept messages, offload their payloads to the
configured storage, and replace the original payload with a small JSON object containing the metadata needed to retrieve
the original message.

Consumers can then use a corresponding deserializer or another SMT to retrieve the full message content from the
external storage using the claim check.

## Future Plans

This toolkit is designed with extensibility in mind. While it currently features the ClaimCheck SMT, we plan to
introduce other useful SMTs in the future to address various Kafka Connect data transformation needs.

## Getting Started

### Building the Project

To build the project, navigate to the root directory and execute the Gradle build command:

```bash
./gradlew clean shadowJar
```

This will compile the SMTs and package them into a JAR file, typically found in `build/libs/`.

### Installation

After building, you can install the SMT plugin into your Kafka Connect environment. Copy the generated JAR file (and its
dependencies, if any) to a directory that is part of your Kafka Connect worker's plugin path.

For example:

```bash
cp build/libs/kafka-connect-claim-check-smt-*.jar /path/to/your/kafka-connect/plugins/
```

Remember to restart your Kafka Connect workers after adding the plugin.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

This project is licensed under the [LICENSE](LICENSE) file.
