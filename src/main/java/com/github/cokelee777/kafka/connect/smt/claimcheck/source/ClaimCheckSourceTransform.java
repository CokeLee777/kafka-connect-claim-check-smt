package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializerFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckReference;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.RecordValueType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * A Kafka Connect Single Message Transform (SMT) for implementing the Claim Check pattern.
 *
 * <p>This transform intercepts outgoing source records. If a record's value payload exceeds a
 * configured size threshold, it is stored in an external storage system (e.g., S3), and the
 * original payload is replaced with a "claim check" reference to that stored data.
 *
 * <p>Records with payloads smaller than the threshold are passed through unmodified. This is
 * intended for use with source connectors.
 */
public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  public static final class Config {

    public static final String STORAGE_TYPE = "storage.type";
    public static final String THRESHOLD_BYTES = "threshold.bytes";

    /** Default threshold: 1MB (1024 * 1024 bytes) */
    private static final int DEFAULT_THRESHOLD_BYTES = 1024 * 1024;

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                STORAGE_TYPE,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(StorageType.S3.type()),
                ConfigDef.Importance.HIGH,
                "Storage implementation type")
            .define(
                THRESHOLD_BYTES,
                ConfigDef.Type.INT,
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
  private int thresholdBytes;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;

  public ClaimCheckSourceTransform() {}

  public ClaimCheckStorage getStorage() {
    return this.storage;
  }

  public int getThresholdBytes() {
    return this.thresholdBytes;
  }

  /**
   * Configures this transform.
   *
   * @param configs The configuration settings.
   */
  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getInt(Config.THRESHOLD_BYTES);
    this.storageType = config.getString(Config.STORAGE_TYPE);

    this.storage = ClaimCheckStorageFactory.create(this.storageType);
    this.storage.configure(configs);

    this.recordSerializer = RecordSerializerFactory.create();
  }

  /**
   * Applies the claim check logic to a source record.
   *
   * <p>If the record's value payload is larger than the configured threshold, it is uploaded to the
   * backend storage, and the record's value is replaced with a claim check reference. Otherwise,
   * the record is returned unchanged.
   *
   * @param record The source record to process.
   * @return A new record with a claim check if the payload was oversized, or the original record.
   */
  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      return record;
    }

    byte[] serializedRecord = serializeRecord(record);
    if (serializedRecord == null || serializedRecord.length <= this.thresholdBytes) {
      return record;
    }

    return createClaimCheckRecord(record, serializedRecord);
  }

  private byte[] serializeRecord(SourceRecord record) {
    if (this.recordSerializer == null) {
      throw new IllegalStateException("RecordSerializer not configured");
    }
    return this.recordSerializer.serialize(record);
  }

  private SourceRecord createClaimCheckRecord(SourceRecord record, byte[] serializedRecord) {
    String referenceUrl = this.storage.store(serializedRecord);
    RecordValueType recordValueType = RecordValueType.from(record);
    Struct referenceValue =
        ClaimCheckReference.create(referenceUrl, recordValueType, serializedRecord.length)
            .toStruct();

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        ClaimCheckSchema.SCHEMA,
        referenceValue,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return Config.DEFINITION;
  }

  @Override
  public void close() {
    if (storage != null) {
      storage.close();
    }
  }
}
