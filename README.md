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

| Connector                               | Tested Version | Status     | Notes                                                                      |
|-----------------------------------------|----------------|------------|----------------------------------------------------------------------------|
| **Debezium MySQL CDC Source Connector** | 2.1.4          | ✅ Verified | Tested with Debezium CDC envelope (before/after/source/op) and deeply nested schemas |
| **Confluent JDBC Source Connector**     | 10.7.6         | ✅ Verified | Tested with complex nested Struct schemas (non-CDC, snapshot-based records) |

**Expected to Work (Not Yet Tested):**

| Connector                                    | Expected Compatibility | Notes                                                       |
|----------------------------------------------|------------------------|-------------------------------------------------------------|
| **Debezium PostgreSQL CDC Source Connector** | 2.x                    | Should work with similar CDC envelope structure             |
| **Debezium Oracle CDC Source Connector**     | 2.x                    | Should work with similar CDC envelope structure             |
| **Custom Source Connectors**                 | Any                    | Must produce `org.apache.kafka.connect.data.Struct` records |

> **Note:** If you test this SMT with other connectors, please consider contributing your findings via GitHub issues or
> pull requests!

**Kafka Connect Version Compatibility:**

**Tested Environment:**

- ✅ **Confluent Platform 7.6.1** (includes Apache Kafka 3.6.x) - Verified
- ✅ Built against **Kafka Connect API 3.8.1** for forward compatibility

**Expected Compatibility:**

- ✅ **Kafka Connect 2.0 - 3.8.x**: Should be compatible (uses stable Connect API)
- ✅ **Kafka Connect 3.9+**: Expected to be compatible
- ✅ **Confluent Platform 7.x**: Should be compatible with all 7.x versions
- ⚠️ **Kafka Connect 1.x**: Not tested, may require modifications

> **Note:** The SMT is built against Kafka Connect API 3.8.1 but tested on Confluent Platform 7.6.1 (Kafka 3.6.x). The
> backward compatibility of Connect API allows newer builds to work on older runtime versions.

**Technical Requirements:**

The ClaimCheck SMT operates at the **pre-serialization stage** of the Kafka Connect pipeline:

```
Source Connector → SMT (ClaimCheck) → Converter (JSON/Avro/Protobuf) → Kafka Broker
```

This means:

- ✅ Works **independently of Converter choice** (JSON, Avro, Protobuf, etc.)
- ✅ Processes data as Java objects (`Struct`), not serialized bytes
- ✅ Compatible with any connector producing `Schema + Struct` records
- ❌ Does **not** work with schema-less connectors that produce raw `Map<String, Object>` or primitive types

#### Configuration

To use the ClaimCheck SMT, you'll need to configure it in your Kafka Connect connector. Below is an example
configuration snippet for a source connector, demonstrating how to apply the `ClaimCheckSourceTransform`.

```jsonc
{
  "name": "my-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "topic.prefix": "my-prefix-",
    "transforms": "claimcheck",
    "transforms.claimcheck.type": "com.github.cokelee777.kafka.connect.smt.claimcheck.source.ClaimCheckSourceTransform",
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

**ClaimCheck SMT Configuration Properties:**

* `threshold.bytes`: (Optional) Messages larger than this size (in bytes) will be offloaded to external storage.
  Defaults to 1MB (1048576 bytes).
* `storage.type`: (Required) The type of storage backend to use (e.g., `s3`).
* `storage.s3.bucket.name`: (Required) The name of the S3 bucket to store the offloaded messages.
* `storage.s3.region`: (Optional) The AWS region of the S3 bucket. Defaults to `ap-northeast-2`.
* `storage.s3.path.prefix`: (Optional) The prefix (directory path) within the S3 bucket where offloaded messages will be
  stored. Defaults to `claim-checks`. Offloaded objects are stored directly under this prefix and named with a randomly
  generated UUID (no original message filename or key is preserved).
  Example object key: `<prefix>/<uuid>` (e.g. `claim-checks/3f2a1b4e-9c7d-4a2f-8b6e-0d1c2e3f4a5b`).
* `storage.s3.retry.max`: (Optional) The maximum number of retry attempts for S3 upload failures. This value specifies
  the number of retries excluding the initial attempt. Defaults to `3`. Set to `0` to disable retries and fail
  immediately on the first upload error.
* `storage.s3.retry.backoff.ms`: (Optional) The initial backoff delay (in milliseconds) before retrying a failed S3
  upload. This value is used as the base delay for exponential backoff. Defaults to `300` milliseconds.
* `storage.s3.retry.max.backoff.ms`: (Optional) The maximum backoff delay (in milliseconds) between S3 upload retries.
  Even if the exponential backoff calculation exceeds this value, the delay will be capped at this maximum. Defaults
  to `20000` milliseconds (20 seconds).

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
