package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.common.utils.AutoCloseableUtils;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Headers;
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

  public ClaimCheckSourceTransform() {}

  ClaimCheckSourceTransformConfig getConfig() {
    return config;
  }

  ClaimCheckStorage getStorage() {
    return storage;
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
    return applyClaimCheck(record, RecordValueDefaults.forSchemaless());
  }

  private SourceRecord applyWithSchema(SourceRecord record) {
    return applyClaimCheck(record, RecordValueDefaults.forSchema(record.valueSchema()));
  }

  private SourceRecord applyClaimCheck(SourceRecord record, Object placeholderValue) {
    byte[] serializedRecord = RecordValueSerializer.serialize(record);
    int recordSizeBytes = serializedRecord != null ? serializedRecord.length : 0;
    if (skipClaimCheck(recordSizeBytes)) {
      return record;
    }

    String referenceUrl = storage.store(serializedRecord);
    SchemaAndValue claimCheckHeader =
        ClaimCheckHeader.toHeader(ClaimCheckMetadata.create(referenceUrl, recordSizeBytes));

    Headers updatedHeaders = record.headers().duplicate();
    updatedHeaders.add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        placeholderValue,
        record.timestamp(),
        updatedHeaders);
  }

  private boolean skipClaimCheck(int recordSizeBytes) {
    int thresholdBytes = config.getThresholdBytes();
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
