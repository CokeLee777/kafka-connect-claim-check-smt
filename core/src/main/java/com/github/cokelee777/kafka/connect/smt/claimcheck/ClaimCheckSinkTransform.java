package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializerFactory;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SMT that restores original payloads from external storage using claim check references.
 *
 * <p>When a record contains a claim check header, the original payload is retrieved from external
 * storage and the record value is restored.
 */
public class ClaimCheckSinkTransform implements Transformation<SinkRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSinkTransform.class);

  public static class Config {

    public static final String STORAGE_TYPE = "storage.type";

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                STORAGE_TYPE,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(
                    ClaimCheckStorageType.S3.type(), ClaimCheckStorageType.FILESYSTEM.type()),
                ConfigDef.Importance.HIGH,
                "Storage implementation type (s3, filesystem)");

    private Config() {}
  }

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(ClaimCheckSinkTransform.Config.DEFINITION, originals);
    }
  }

  private String storageType;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;

  public ClaimCheckSinkTransform() {}

  public ClaimCheckSinkTransform(ClaimCheckStorage storage) {
    this.storage = storage;
  }

  public ClaimCheckSinkTransform(RecordSerializer recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  public ClaimCheckSinkTransform(ClaimCheckStorage storage, RecordSerializer recordSerializer) {
    this.storage = storage;
    this.recordSerializer = recordSerializer;
  }

  public String getStorageType() {
    return storageType;
  }

  public ClaimCheckStorage getStorage() {
    return storage;
  }

  public RecordSerializer getRecordSerializer() {
    return recordSerializer;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.storageType = config.getString(Config.STORAGE_TYPE);

    if (this.storage == null) {
      this.storage = ClaimCheckStorageFactory.create(this.storageType);
    }
    Objects.requireNonNull(this.storage, "ClaimCheckStorage not configured");
    this.storage.configure(configs);

    if (this.recordSerializer == null) {
      this.recordSerializer = RecordSerializerFactory.create();
    }
    Objects.requireNonNull(this.recordSerializer, "RecordSerializer not configured");
  }

  @Override
  public SinkRecord apply(SinkRecord record) {
    if (record == null) {
      return null;
    }

    Header claimCheckHeader = record.headers().lastWithName(ClaimCheckSchema.NAME);
    if (claimCheckHeader == null || claimCheckHeader.value() == null) {
      log.debug("No claim-check header or value found for record from topic: {}", record.topic());
      return record;
    }

    return restoreOriginalRecord(record, claimCheckHeader.value());
  }

  private SinkRecord restoreOriginalRecord(SinkRecord record, Object headerValue) {
    ClaimCheckValue claimCheckValue = parseClaimCheckValue(headerValue);
    byte[] originalRecordBytes = retrieveOriginalRecord(claimCheckValue);
    SchemaAndValue schemaAndValue = deserializeRecord(record.topic(), originalRecordBytes);
    if (schemaAndValue == null) {
      throw new ConnectException(
          "Failed to restore original record from claim check value (topic="
              + record.topic()
              + ")");
    }

    return buildRestoredRecord(record, schemaAndValue);
  }

  private ClaimCheckValue parseClaimCheckValue(Object headerValue) {
    return ClaimCheckValue.from(headerValue);
  }

  private byte[] retrieveOriginalRecord(ClaimCheckValue claimCheckValue) {
    String referenceUrl = claimCheckValue.getReferenceUrl();
    int originalSizeBytes = claimCheckValue.getOriginalSizeBytes();

    log.debug(
        "Recovering claim check record from: {}, original size: {} bytes",
        referenceUrl,
        originalSizeBytes);

    byte[] originalRecordBytes = this.storage.retrieve(referenceUrl);
    validateRetrievedPayload(originalRecordBytes, referenceUrl, originalSizeBytes);
    return originalRecordBytes;
  }

  private void validateRetrievedPayload(
      byte[] originalRecordBytes, String referenceUrl, int originalSizeBytes) {
    if (originalRecordBytes == null) {
      throw new ConnectException("Failed to retrieve data from: " + referenceUrl);
    }

    if (originalRecordBytes.length == 0 && originalSizeBytes > 0) {
      throw new ConnectException("Retrieved empty data from: " + referenceUrl);
    }

    if (originalRecordBytes.length != originalSizeBytes) {
      throw new ConnectException(
          String.format(
              "Data integrity violation: size mismatch for %s (expected: %d bytes, retrieved: %d bytes)",
              referenceUrl, originalSizeBytes, originalRecordBytes.length));
    }
  }

  private SchemaAndValue deserializeRecord(String topic, byte[] originalRecordBytes) {
    return this.recordSerializer.deserialize(topic, originalRecordBytes);
  }

  private SinkRecord buildRestoredRecord(SinkRecord record, SchemaAndValue schemaAndValue) {
    SinkRecord originalRecord =
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schemaAndValue.schema(),
            schemaAndValue.value(),
            record.timestamp());

    log.debug("Successfully recovered claim check record.");

    originalRecord.headers().remove(ClaimCheckSchema.NAME);
    return originalRecord;
  }

  @Override
  public ConfigDef config() {
    return Config.DEFINITION;
  }

  @Override
  public void close() {
    if (this.storage != null) {
      this.storage.close();
    }
  }
}
