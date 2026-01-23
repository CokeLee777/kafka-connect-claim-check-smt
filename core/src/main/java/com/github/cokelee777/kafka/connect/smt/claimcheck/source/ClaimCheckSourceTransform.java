package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializerFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckReference;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

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

  public ClaimCheckSourceTransform(ClaimCheckStorage storage) {
    this.storage = storage;
  }

  public ClaimCheckSourceTransform(RecordSerializer recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  public ClaimCheckSourceTransform(ClaimCheckStorage storage, RecordSerializer recordSerializer) {
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

  public int getThresholdBytes() {
    return this.thresholdBytes;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getInt(Config.THRESHOLD_BYTES);
    this.storageType = config.getString(Config.STORAGE_TYPE);

    if (this.storage == null) {
      this.storage = ClaimCheckStorageFactory.create(this.storageType);
      this.storage.configure(configs);
    }

    if (this.recordSerializer == null) {
      this.recordSerializer = RecordSerializerFactory.create();
    }
  }

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
    Struct referenceValue =
        ClaimCheckReference.create(referenceUrl, serializedRecord.length).toStruct();

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
