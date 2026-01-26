package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.DefaultValueStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.DefaultValueStrategySelector;
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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);

  public static final class Config {

    public static final String STORAGE_TYPE = "storage.type";
    public static final String THRESHOLD_BYTES = "threshold.bytes";

    /** Default threshold: 1MB (1024 * 1024 bytes) */
    private static final long DEFAULT_THRESHOLD_BYTES = 1024L * 1024L;

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                STORAGE_TYPE,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(ClaimCheckStorageType.S3.type()),
                ConfigDef.Importance.HIGH,
                "Storage implementation type")
            .define(
                THRESHOLD_BYTES,
                ConfigDef.Type.LONG,
                DEFAULT_THRESHOLD_BYTES,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.HIGH,
                "Payload size threshold in bytes");

    private Config() {}
  }

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(Config.DEFINITION, originals);
    }
  }

  private String storageType;
  private long thresholdBytes;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;
  private DefaultValueStrategySelector defaultValueStrategySelector;

  public ClaimCheckSourceTransform() {}

  ClaimCheckSourceTransform(ClaimCheckStorage storage) {
    this.storage = storage;
  }

  ClaimCheckSourceTransform(RecordSerializer recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  ClaimCheckSourceTransform(ClaimCheckStorage storage, RecordSerializer recordSerializer) {
    this.storage = storage;
    this.recordSerializer = recordSerializer;
  }

  public ClaimCheckStorage getStorage() {
    return this.storage;
  }

  public String getStorageType() {
    return storageType;
  }

  public RecordSerializer getRecordSerializer() {
    return recordSerializer;
  }

  public long getThresholdBytes() {
    return this.thresholdBytes;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getLong(Config.THRESHOLD_BYTES);
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

    this.defaultValueStrategySelector = new DefaultValueStrategySelector();
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      log.debug("Skipping claim check: record value is null");
      return record;
    }

    byte[] originalRecordBytes = serializeRecord(record);
    if (originalRecordBytes == null || originalRecordBytes.length <= this.thresholdBytes) {
      log.debug(
          "Record size {} below threshold {}, skipping claim check",
          originalRecordBytes != null ? originalRecordBytes.length : 0,
          this.thresholdBytes);
      return record;
    }

    log.debug(
        "Record size {} exceeds threshold {}, applying claim check",
        originalRecordBytes.length,
        this.thresholdBytes);
    return createClaimCheckRecord(record, originalRecordBytes);
  }

  private byte[] serializeRecord(SourceRecord record) {
    return this.recordSerializer.serialize(record);
  }

  private SourceRecord createClaimCheckRecord(SourceRecord record, byte[] originalRecordBytes) {
    String referenceUrl = storeOriginalRecord(originalRecordBytes);
    Struct claimCheckValue = createClaimCheckValue(referenceUrl, originalRecordBytes.length);
    Object defaultValue = createDefaultValue(record);
    return buildClaimCheckRecord(record, claimCheckValue, defaultValue);
  }

  private String storeOriginalRecord(byte[] originalRecordBytes) {
    String referenceUrl = this.storage.store(originalRecordBytes);
    log.debug(
        "Stored original record. Size: {} bytes, Reference URL: {}",
        originalRecordBytes.length,
        referenceUrl);
    return referenceUrl;
  }

  private Struct createClaimCheckValue(String referenceUrl, int originalSizeBytes) {
    return ClaimCheckValue.create(referenceUrl, originalSizeBytes).toStruct();
  }

  private Object createDefaultValue(SourceRecord record) {
    DefaultValueStrategy strategy = this.defaultValueStrategySelector.selectStrategy(record);
    log.debug(
        "Applying default value with strategy: '{}' for topic: '{}'",
        strategy.getStrategyType(),
        record.topic());
    return strategy.createDefaultValue(record);
  }

  private SourceRecord buildClaimCheckRecord(
      SourceRecord record, Struct claimCheckValue, Object defaultValue) {
    SourceRecord sourceRecord =
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            defaultValue,
            record.timestamp());

    sourceRecord.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);
    return sourceRecord;
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
