package com.github.cokelee777.kafka.connect.smt.claimcheck.sink;

import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordSerializerFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchemaFields;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                ConfigDef.ValidString.in(StorageType.S3.type()),
                ConfigDef.Importance.HIGH,
                "Storage implementation type");

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
    if (record.value() == null || record.valueSchema() == null) {
      return record;
    }

    if (!(ClaimCheckSchema.SCHEMA.name().equals(record.valueSchema().name()))) {
      return record;
    }

    if (!(record.value() instanceof Struct)) {
      return record;
    }

    Struct claimCheckReferenceStruct = (Struct) record.value();
    String referenceUrl = claimCheckReferenceStruct.getString(ClaimCheckSchemaFields.REFERENCE_URL);
    byte[] serializedRecord = this.storage.retrieve(referenceUrl);

    SchemaAndValue schemaAndValue =
        this.recordSerializer.deserialize(record.topic(), serializedRecord);
    if (schemaAndValue == null) {
      log.warn(
          "Failed to restore original record from claim check reference (topic={}). Returning original record.",
          record.topic());
      return record;
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        schemaAndValue.schema(),
        schemaAndValue.value(),
        record.timestamp());
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
