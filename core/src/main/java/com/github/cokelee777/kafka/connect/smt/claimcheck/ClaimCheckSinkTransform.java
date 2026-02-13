package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializerFactory;
import com.github.cokelee777.kafka.connect.smt.common.utils.AutoCloseableUtils;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
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

  private ClaimCheckSinkTransformConfig config;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;

  public ClaimCheckSinkTransform() {}

  ClaimCheckSinkTransformConfig getConfig() {
    return config;
  }

  ClaimCheckStorage getStorage() {
    return storage;
  }

  RecordSerializer getRecordSerializer() {
    return recordSerializer;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new ClaimCheckSinkTransformConfig(configs);

    // Allow test injection of storage
    if (storage == null) {
      String storageType = config.getStorageType();
      storage = ClaimCheckStorageFactory.create(storageType);
    }
    storage.configure(configs);

    recordSerializer = RecordSerializerFactory.create();

    log.info("ClaimCheck sink transform configured: storage={}", config.getStorageType());
  }

  @Override
  public SinkRecord apply(SinkRecord record) {
    if (record == null) {
      return null;
    }

    Header claimCheckHeader = record.headers().lastWithName(ClaimCheckSchema.NAME);
    if (claimCheckHeader == null || claimCheckHeader.value() == null) {
      if (log.isDebugEnabled()) {
        log.debug("No claim-check header or value found for record from topic: {}", record.topic());
      }
      return record;
    }

    return restoreOriginalRecord(record, claimCheckHeader.value());
  }

  private SinkRecord restoreOriginalRecord(SinkRecord record, Object headerValue) {
    ClaimCheckValue claimCheckValue = parseClaimCheckValue(headerValue);
    byte[] originalRecordBytes = retrieveOriginalRecord(claimCheckValue);
    SchemaAndValue schemaAndValue = deserializeRecord(record.topic(), originalRecordBytes);
    if (schemaAndValue == null) {
      throw new DataException(
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
    String referenceUrl = claimCheckValue.referenceUrl();
    int originalSizeBytes = claimCheckValue.originalSizeBytes();

    if (log.isDebugEnabled()) {
      log.debug(
          "Recovering claim check record from: {}, original size: {} bytes",
          referenceUrl,
          originalSizeBytes);
    }

    byte[] originalRecordBytes = storage.retrieve(referenceUrl);
    validateRetrievedPayload(originalRecordBytes, referenceUrl, originalSizeBytes);
    return originalRecordBytes;
  }

  private void validateRetrievedPayload(
      byte[] originalRecordBytes, String referenceUrl, int originalSizeBytes) {
    if (originalRecordBytes == null) {
      throw new ConnectException("Failed to retrieve data from: " + referenceUrl);
    }

    if (originalRecordBytes.length == 0 && originalSizeBytes > 0) {
      throw new DataException("Retrieved empty data from: " + referenceUrl);
    }

    if (originalRecordBytes.length != originalSizeBytes) {
      throw new DataException(
          String.format(
              "Data integrity violation: size mismatch for %s (expected: %d bytes, retrieved: %d bytes)",
              referenceUrl, originalSizeBytes, originalRecordBytes.length));
    }
  }

  private SchemaAndValue deserializeRecord(String topic, byte[] originalRecordBytes) {
    return recordSerializer.deserialize(topic, originalRecordBytes);
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
    return ClaimCheckSinkTransformConfig.configDef();
  }

  @Override
  public void close() {
    if (storage instanceof AutoCloseable autoCloseable) {
      AutoCloseableUtils.closeQuietly(autoCloseable);
    }
  }
}
