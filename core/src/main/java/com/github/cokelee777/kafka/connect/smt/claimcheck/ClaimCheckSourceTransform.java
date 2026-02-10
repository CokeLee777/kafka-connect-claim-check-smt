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
import java.util.Objects;
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

  public ClaimCheckSourceTransformConfig getConfig() {
    return config;
  }

  public ClaimCheckStorage getStorage() {
    return storage;
  }

  public RecordSerializer getRecordSerializer() {
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
    Objects.requireNonNull(storage, "ClaimCheckStorage not configured");
    storage.configure(configs);

    recordSerializer = RecordSerializerFactory.create();
    Objects.requireNonNull(recordSerializer, "RecordSerializer not configured");
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      log.debug("Skipping claim check: record value is null");
      return record;
    }

    byte[] originalRecordBytes = serializeRecord(record);
    int thresholdBytes = config.getThresholdBytes();
    if (originalRecordBytes == null || originalRecordBytes.length <= thresholdBytes) {
      log.debug(
          "Record size {} below threshold {}, skipping claim check",
          originalRecordBytes != null ? originalRecordBytes.length : 0,
          thresholdBytes);
      return record;
    }

    log.debug(
        "Record size {} exceeds threshold {}, applying claim check",
        originalRecordBytes.length,
        thresholdBytes);
    return createClaimCheckRecord(record, originalRecordBytes);
  }

  private byte[] serializeRecord(SourceRecord record) {
    return recordSerializer.serialize(record);
  }

  private SourceRecord createClaimCheckRecord(SourceRecord record, byte[] originalRecordBytes) {
    String referenceUrl = storeOriginalRecord(originalRecordBytes);
    Struct claimCheckValue = createClaimCheckValue(referenceUrl, originalRecordBytes.length);
    Object placeholder = createPlaceholder(record);
    return buildClaimCheckRecord(record, claimCheckValue, placeholder);
  }

  private String storeOriginalRecord(byte[] originalRecordBytes) {
    String referenceUrl = storage.store(originalRecordBytes);
    log.debug(
        "Stored original record. Size: {} bytes, Reference URL: {}",
        originalRecordBytes.length,
        referenceUrl);
    return referenceUrl;
  }

  private Struct createClaimCheckValue(String referenceUrl, int originalSizeBytes) {
    return ClaimCheckValue.create(referenceUrl, originalSizeBytes).toStruct();
  }

  private Object createPlaceholder(SourceRecord record) {
    RecordValuePlaceholder strategy = RecordValuePlaceholderResolver.resolve(record);
    log.debug(
        "Applying placeholder with strategy: '{}' for topic: '{}'",
        strategy.getPlaceholderType(),
        record.topic());
    return strategy.apply(record);
  }

  private SourceRecord buildClaimCheckRecord(
      SourceRecord record, Struct claimCheckValue, Object placeholder) {
    SourceRecord sourceRecord =
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            placeholder,
            record.timestamp());

    sourceRecord.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);
    return sourceRecord;
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
