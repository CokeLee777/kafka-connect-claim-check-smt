package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.RecordValuePlaceholderResolver;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type.RecordValuePlaceholder;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializerFactory;
import com.github.cokelee777.kafka.connect.smt.common.utils.AutoCloseableUtils;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SMT that offloads large payloads to external storage and replaces them with a claim check
 * reference.
 *
 * <p>When a record exceeds the configured threshold, its payload is stored externally (e.g., S3)
 * and the record value is replaced with a placeholder while the reference URL is added to the
 * headers.
 */
public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);

  private ClaimCheckSourceTransformConfig config;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;

  public ClaimCheckSourceTransform() {}

  ClaimCheckSourceTransformConfig getConfig() {
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
    config = new ClaimCheckSourceTransformConfig(configs);

    // Allow test injection of storage
    if (storage == null) {
      String storageType = config.getStorageType();
      storage = ClaimCheckStorageFactory.create(storageType);
    }
    storage.configure(configs);

    recordSerializer = RecordSerializerFactory.create();

    log.info(
        "ClaimCheck source transform configured: threshold={}bytes, storage={}",
        config.getThresholdBytes(),
        config.getStorageType());
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private SourceRecord applySchemaless(SourceRecord record) {
    final byte[] serializedRecord = recordSerializer.serialize(record);
    final int recordSizeBytes = serializedRecord != null ? serializedRecord.length : 0;
    if (skipClaimCheck(recordSizeBytes)) {
      return record;
    }

    final String referenceUrl = storeRecord(serializedRecord);
    final Struct claimCheckValue = createClaimCheckValue(referenceUrl, recordSizeBytes);
    final Object placeholderRecordValue = createPlaceholderRecordValue(record);

    final SourceRecord newRecord =
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            null,
            placeholderRecordValue,
            record.timestamp());

    newRecord.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);
    return newRecord;
  }

  private SourceRecord applyWithSchema(SourceRecord record) {
    final byte[] serializedRecord = recordSerializer.serialize(record);
    final int recordSizeBytes = serializedRecord != null ? serializedRecord.length : 0;
    if (skipClaimCheck(recordSizeBytes)) {
      return record;
    }

    final String referenceUrl = storeRecord(serializedRecord);
    final Struct claimCheckValue = createClaimCheckValue(referenceUrl, recordSizeBytes);
    final Object placeholderRecordValue = createPlaceholderRecordValue(record);

    final SourceRecord newRecord =
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            placeholderRecordValue,
            record.timestamp());

    newRecord.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);
    return newRecord;
  }

  private boolean skipClaimCheck(int recordSizeBytes) {
    final int thresholdBytes = config.getThresholdBytes();
    if (recordSizeBytes <= thresholdBytes) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Record size {} bytes is below threshold {} bytes, skipping claim check",
            recordSizeBytes,
            thresholdBytes);
      }
      return true;
    }

    if (log.isDebugEnabled()) {
      log.debug(
          "Record size {} bytes exceeds threshold {} bytes, applying claim check",
          recordSizeBytes,
          thresholdBytes);
    }
    return false;
  }

  private String storeRecord(byte[] serializedRecord) {
    final String referenceUrl = storage.store(serializedRecord);

    if (log.isDebugEnabled()) {
      log.debug(
          "Stored record of {} bytes at reference: {}", serializedRecord.length, referenceUrl);
    }

    return referenceUrl;
  }

  private Struct createClaimCheckValue(String referenceUrl, int recordSizeBytes) {
    return ClaimCheckValue.create(referenceUrl, recordSizeBytes).toStruct();
  }

  private Object createPlaceholderRecordValue(SourceRecord record) {
    final RecordValuePlaceholder placeholder = RecordValuePlaceholderResolver.resolve(record);

    if (log.isDebugEnabled()) {
      log.debug(
          "Applying placeholder strategy '{}' for topic '{}'",
          placeholder.getClass().getSimpleName(),
          record.topic());
    }

    return placeholder.apply(record);
  }

  @Override
  public ConfigDef config() {
    return ClaimCheckSourceTransformConfig.configDef();
  }

  @Override
  public void close() {
    if (storage instanceof AutoCloseable autoCloseable) {
      AutoCloseableUtils.closeQuietly(autoCloseable);
    }
  }
}
