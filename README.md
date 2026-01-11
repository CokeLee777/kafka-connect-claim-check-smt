# Kafka Connect SMT Toolkit

This repository hosts an open-source toolkit for Kafka Connect Single Message Transforms (SMTs). It provides a collection of reusable SMTs designed to enhance data processing capabilities within Kafka Connect pipelines.

## Technology Stack

- **Java 11** - Core programming language
- **Kafka Connect** - Kafka's framework for connecting Kafka with external systems
- **AWS SDK for Java** - Amazon S3 integration for storage backend
- **Gradle** - Build automation tool with Shadow plugin for uber JAR creation

## Features

### ClaimCheck SMT

The initial SMT included in this toolkit is the **ClaimCheck SMT**. This transform allows you to offload large message payloads from Kafka topics to external storage (like Amazon S3), replacing them with a "claim check" (a reference to the original data). This helps in reducing Kafka message sizes, improving throughput, and lowering storage costs for Kafka brokers, while still allowing consumers to retrieve the full message content when needed.

**Current Storage Backends:**
*   Amazon S3

**Supported Connectors:**

The ClaimCheck SMT works with any Kafka Connect **Source Connector** that produces structured data (Schema + Struct). It has been tested with:

*   **Debezium MySQL CDC Connector** - Change Data Capture for MySQL databases
*   **Debezium PostgreSQL CDC Connector** - Change Data Capture for PostgreSQL databases
*   **JDBC Source Connector** - Bulk and incremental data ingestion from relational databases
*   Any other Source Connector that produces `org.apache.kafka.connect.data.Struct` records

**Note:** The ClaimCheck SMT operates on `SourceRecord` objects before they are serialized by Converters, working directly with Kafka Connect's internal `Schema` and `Struct` data structures.

#### Configuration

To use the ClaimCheck SMT, you'll need to configure it in your Kafka Connect connector. Below is an example configuration snippet for a source connector, demonstrating how to apply the `ClaimCheckSourceTransform`.

```json
{
  "name": "my-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "topic.prefix": "my-prefix-",
    "transforms": "claimcheck",
    "transforms.claimcheck.type": "com.github.cokelee777.kafka.connect.smt.claimcheck.source.ClaimCheckSourceTransform",
    "transforms.claimcheck.threshold.bytes": "1048576",
    "transforms.claimcheck.storage.type": "S3",
    "transforms.claimcheck.s3.bucket.name": "your-s3-bucket-name",
    "transforms.claimcheck.s3.region": "your-aws-region"
    // ... other connector configurations
  }
}
```

**ClaimCheck SMT Configuration Properties:**

*   `threshold.bytes`: (Optional) Messages larger than this size (in bytes) will be offloaded to external storage. Defaults to 1MB (1048576 bytes).
*   `storage.type`: The type of storage backend to use (e.g., `S3`).
*   `s3.bucket.name`: (Required for S3) The name of the S3 bucket to store the offloaded messages.
*   `s3.region`: (Required for S3) The AWS region of the S3 bucket.

#### Usage

Once configured and deployed, the ClaimCheck SMT will automatically intercept messages, offload their payloads to the configured storage, and replace the original payload with a small JSON object containing the metadata needed to retrieve the original message.

Consumers can then use a corresponding deserializer or another SMT to retrieve the full message content from the external storage using the claim check.

## Future Plans

This toolkit is designed with extensibility in mind. While it currently features the ClaimCheck SMT, we plan to introduce other useful SMTs in the future to address various Kafka Connect data transformation needs.

## Getting Started

### Building the Project

To build the project, navigate to the root directory and execute the Gradle build command:

```bash
./gradlew clean shadowJar
```

This will compile the SMTs and package them into a JAR file, typically found in `build/libs/`.

### Installation

After building, you can install the SMT plugin into your Kafka Connect environment. Copy the generated JAR file (and its dependencies, if any) to a directory that is part of your Kafka Connect worker's plugin path.

For example:

```bash
cp build/libs/kafka-connect-smt-toolkit-*.jar /path/to/your/kafka-connect/plugins/
```

Remember to restart your Kafka Connect workers after adding the plugin.



## Contributing

We welcome contributions to this open-source project! If you have ideas for new SMTs, improvements to existing ones, or bug fixes, please feel free to open an issue or submit a pull request.

## License

This project is licensed under the [LICENSE](LICENSE) file.
