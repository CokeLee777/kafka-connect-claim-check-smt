# Kafka Connect SMT Toolkit

This repository hosts an open-source toolkit for Kafka Connect Single Message Transforms (SMTs). It provides a
collection of reusable SMTs designed to enhance data processing capabilities within Kafka Connect pipelines.

## Technology Stack

### Core Technologies

- **Java 11** - Target and source compatibility
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

**Supported Connectors:**

The ClaimCheck SMT is compatible with **Kafka Connect 2.0+** and works with any **Source Connector** that produces
structured data using Kafka Connect's `Schema` and `Struct` API.

**Tested and Verified:**

*Source Connectors (ClaimCheckSourceTransform):*

| Connector                               | Tested Version | Status     | Notes                                                                                |
|-----------------------------------------|----------------|------------|--------------------------------------------------------------------------------------|
| **Debezium MySQL CDC Source Connector** | 2.1.4          | ✅ Verified | Tested with Debezium CDC envelope (before/after/source/op) and deeply nested schemas |
| **Confluent JDBC Source Connector**     | 10.7.6         | ✅ Verified | Tested with complex nested Struct schemas (non-CDC, snapshot-based records)          |

*Sink Connectors (ClaimCheckSinkTransform):*

| Connector                         | Tested Version | Status     | Notes                                                                                                       |
|-----------------------------------|----------------|------------|-------------------------------------------------------------------------------------------------------------|
| **Confluent JDBC Sink Connector** | 10.7.6         | ✅ Verified | Tested with restored Struct records from claim check references, including primitive and nested field types |

> **Note:** If you test this SMT with other connectors, please consider contributing your findings via GitHub issues or
> pull requests!

**Kafka Connect Version Compatibility:**

**Tested Environment:**

- ✅ **Confluent Platform 7.6.1** (includes Apache Kafka 3.6.x) - Verified
- ✅ Built against **Kafka Connect API 3.8.1** for forward compatibility

> **Note:** The SMT is built against Kafka Connect API 3.8.1 but tested on Confluent Platform 7.6.1 (Kafka 3.6.x). The
> backward compatibility of Connect API allows newer builds to work on older runtime versions.

**Technical Requirements:**

The ClaimCheck SMT operates at the **pre-serialization stage** of the Kafka Connect pipeline:

```text
Source Connector → SMT (ClaimCheck) → Converter (JSON/Avro/Protobuf) → Kafka Broker
```

```text
Kafka Broker → Converter (JSON/Avro/Protobuf) → SMT (ClaimCheck) → Sink Connector
```

This means:

- ✅ Works **independently of Converter choice** (JSON, Avro, Protobuf, etc.)
- ✅ Processes data as Java objects (`Struct`), not serialized bytes
- ✅ **Full support** for `Schema + Struct` records (Debezium CDC, JDBC Source, etc.)
- ✅ **Partial support** for schemaless records (`Map<String, Object>`) - value is replaced with `null`, original payload
  stored in external storage and restored on sink side
- ❌ Does **not** support primitive type values (e.g., raw `String`, `Integer`, `byte[]`)

#### Configuration

To use the ClaimCheck SMT, you'll need to configure it in your Kafka Connect connector.

##### Source Connector Configuration

Below is an example configuration snippet for a source connector, demonstrating how to apply
the `ClaimCheckSourceTransform`.

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

##### Sink Connector Configuration

Below is an example configuration snippet for a sink connector, demonstrating how to apply
the `ClaimCheckSinkTransform`.

```jsonc
{
  "name": "my-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topic.prefix": "my-prefix-",
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

**ClaimCheck SMT Configuration Properties:**

*ClaimCheckSourceTransform Properties:*

| Property          | Required | Default         | Description                                                                      |
|-------------------|----------|-----------------|----------------------------------------------------------------------------------|
| `threshold.bytes` | No       | `1048576` (1MB) | Messages larger than this size (in bytes) will be offloaded to external storage. |
| `storage.type`    | Yes      | -               | The type of storage backend to use (e.g., `s3`).                                 |

*ClaimCheckSinkTransform Properties:*

| Property       | Required | Default | Description                                                                                      |
|----------------|----------|---------|--------------------------------------------------------------------------------------------------|
| `storage.type` | Yes      | -       | The type of storage backend to use (e.g., `s3`). Must match the source connector's storage type. |

*Common S3 Storage Properties (Both Source and Sink):*

| Property                          | Required | Default          | Description                                                                                                                                                                                          |
|-----------------------------------|----------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `storage.s3.bucket.name`          | Yes      | -                | The name of the S3 bucket to store/retrieve the offloaded messages.                                                                                                                                  |
| `storage.s3.region`               | No       | `ap-northeast-2` | The AWS region of the S3 bucket.                                                                                                                                                                     |
| `storage.s3.path.prefix`          | No       | `claim-checks`   | The prefix (directory path) within the S3 bucket where offloaded messages are stored. Objects are named with a randomly generated UUID. Example: `claim-checks/3f2a1b4e-9c7d-4a2f-8b6e-0d1c2e3f4a5b` |
| `storage.s3.retry.max`            | No       | `3`              | Maximum number of retry attempts for S3 operations (excluding the initial attempt). Set to `0` to disable retries.                                                                                   |
| `storage.s3.retry.backoff.ms`     | No       | `300`            | Initial backoff delay (in milliseconds) before retrying. Used as the base for exponential backoff.                                                                                                   |
| `storage.s3.retry.max.backoff.ms` | No       | `20000`          | Maximum backoff delay (in milliseconds) between retries. Caps the exponential backoff calculation.                                                                                                   |

> **Important:** The Sink connector's S3 configuration (`bucket.name`, `region`, `path.prefix`) must match the Source
> connector's configuration to correctly retrieve the offloaded payloads.

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
cp build/libs/kafka-connect-smt-toolkit-*.jar /path/to/your/kafka-connect/plugins/
```

Remember to restart your Kafka Connect workers after adding the plugin.

## Contributing

We welcome contributions to this open-source project! If you have ideas for new SMTs, improvements to existing ones, or
bug fixes, please feel free to open an issue or submit a pull request.

## License

This project is licensed under the [LICENSE](LICENSE) file.
