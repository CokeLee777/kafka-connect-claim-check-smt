package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.JsonConverterFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.JsonRecordValueSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordValueSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckReference;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.utils.ConfigUtils;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);
  /** Specifies the type of backend storage to use. For example, "s3". */
  public static final String CONFIG_STORAGE_TYPE = "storage.type";
  /**
   * The size threshold in bytes. Payloads larger than this will be offloaded to external storage.
   */
  public static final String CONFIG_THRESHOLD_BYTES = "threshold.bytes";
  private static final int DEFAULT_THRESHOLD_BYTES = 1024 * 1024;
  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              CONFIG_STORAGE_TYPE,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Storage implementation type")
          .define(
              CONFIG_THRESHOLD_BYTES,
              ConfigDef.Type.INT,
              DEFAULT_THRESHOLD_BYTES,
              ConfigDef.Importance.HIGH,
              "Payload size threshold in bytes");

  private int thresholdBytes;
  private ClaimCheckStorage storage;
  private RecordValueSerializer recordValueSerializer;

  public int getThresholdBytes() {
    return thresholdBytes;
  }

  public ClaimCheckStorage getStorage() {
    return storage;
  }

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(CONFIG_DEF, originals);
    }
  }

  public ClaimCheckSourceTransform() {}

  /**
   * Configures this transform.
   *
   * @param configs The configuration settings.
   */
  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getInt(CONFIG_THRESHOLD_BYTES);
    String storageType = ConfigUtils.getRequiredString(config, CONFIG_STORAGE_TYPE);

    this.storage = ClaimCheckStorageFactory.create(storageType);
    this.storage.configure(configs);

    JsonConverter converter = JsonConverterFactory.createValueConverter();
    this.recordValueSerializer = new JsonRecordValueSerializer(converter);

    log.info(
        "ClaimCheckTransform initialized. Threshold: {} bytes, Storage: {}",
        this.thresholdBytes,
        storageType);
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

    byte[] serializedValue = this.recordValueSerializer.serialize(record);
    if (serializedValue == null) {
      return record;
    }

    if (serializedValue.length <= this.thresholdBytes) {
      return record;
    }

    String referenceUrl = this.storage.store(serializedValue);
    Struct referenceStruct =
        ClaimCheckReference.create(referenceUrl, serializedValue.length).toStruct();

    log.info(
        "Payload too large ({} bytes). Uploaded to storage: {}",
        serializedValue.length,
        referenceUrl);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        ClaimCheckSchema.SCHEMA,
        referenceStruct,
        record.timestamp());
  }

  /**
   * Returns the configuration definition for this transform.
   *
   * @return The {@link ConfigDef}.
   */
  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  /** Cleans up any resources used by the transform, such as the storage client. */
  @Override
  public void close() {
    if (storage != null) {
      storage.close();
    }
  }
}
