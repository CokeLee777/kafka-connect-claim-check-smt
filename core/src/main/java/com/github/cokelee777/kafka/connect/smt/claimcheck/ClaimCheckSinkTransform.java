package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.common.utils.AutoCloseableUtils;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
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

  public ClaimCheckSinkTransform() {}

  ClaimCheckSinkTransformConfig getConfig() {
    return config;
  }

  ClaimCheckStorage getStorage() {
    return storage;
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

    log.info("ClaimCheck sink transform configured: storage={}", config.getStorageType());
  }

  @Override
  public SinkRecord apply(SinkRecord record) {
    if (record.value() == null) {
      return record;
    } else {
      Header claimCheckHeader = record.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      return apply(record, claimCheckHeader);
    }
  }

  private SinkRecord apply(SinkRecord record, Header claimCheckHeader) {
    if (skipClaimCheck(claimCheckHeader)) {
      return record;
    }

    return applyClaimCheck(record, claimCheckHeader);
  }

  private boolean skipClaimCheck(Header claimCheckHeader) {
    if (claimCheckHeader == null) {
      if (log.isDebugEnabled()) {
        log.debug("Claim Check Header is null, skipping claim check");
      }
      return true;
    }

    if (claimCheckHeader.value() == null) {
      if (log.isDebugEnabled()) {
        log.debug("Claim Check header value is null, skipping claim check");
      }
      return true;
    }

    return false;
  }

  private SinkRecord applyClaimCheck(SinkRecord record, Header claimCheckHeader) {
    ClaimCheckMetadata claimCheckMetadata = ClaimCheckHeader.fromHeader(claimCheckHeader);
    byte[] originalRecordValueBytes = storage.retrieve(claimCheckMetadata.referenceUrl());
    if (originalRecordValueBytes == null) {
      throw new DataException("Failed to retrieve data from: " + claimCheckMetadata.referenceUrl());
    }

    if (originalRecordValueBytes.length != claimCheckMetadata.originalSizeBytes()) {
      throw new DataException(
          String.format(
              "Data integrity violation: size mismatch for %s (expected: %d bytes, retrieved: %d bytes)",
              claimCheckMetadata.referenceUrl(),
              claimCheckMetadata.originalSizeBytes(),
              originalRecordValueBytes.length));
    }

    Object originalRecordValue =
        RecordValueSerializer.deserialize(record.valueSchema(), originalRecordValueBytes);

    Headers updatedHeaders = record.headers().duplicate();
    updatedHeaders.remove(ClaimCheckHeader.HEADER_KEY);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        originalRecordValue,
        record.timestamp(),
        updatedHeaders);
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
