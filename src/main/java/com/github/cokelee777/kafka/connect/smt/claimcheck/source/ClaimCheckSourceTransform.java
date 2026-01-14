package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.S3Storage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.github.cokelee777.kafka.connect.smt.utils.ConfigUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);

  // Config 상수 정의
  public static final String CONFIG_STORAGE_TYPE = "storage.type";
  public static final String CONFIG_THRESHOLD_BYTES = "threshold.bytes";

  // 기본값 설정 (1MB)
  private static final int DEFAULT_THRESHOLD = 1024 * 1024;
  private static final String DEFAULT_STORAGE_TYPE = "S3";

  // Storage ConfigDef
  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              CONFIG_STORAGE_TYPE,
              ConfigDef.Type.STRING,
              DEFAULT_STORAGE_TYPE,
              ConfigDef.Importance.HIGH,
              "Storage implementation type (e.g., S3, REDIS)")
          .define(
              CONFIG_THRESHOLD_BYTES,
              ConfigDef.Type.INT,
              DEFAULT_THRESHOLD,
              ConfigDef.Importance.HIGH,
              "Payload size threshold in bytes");

  public static final String REFERENCE_SCHEMA_NAME =
      "com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckReference";
  public static final Schema REFERENCE_SCHEMA =
      SchemaBuilder.struct()
          .name(REFERENCE_SCHEMA_NAME)
          .field("reference_url", Schema.STRING_SCHEMA)
          .field("original_size_bytes", Schema.INT64_SCHEMA)
          .field("uploaded_at", Schema.INT64_SCHEMA)
          .build();

  private ClaimCheckStorage storage;
  private int thresholdBytes;
  private JsonConverter jsonConverter;

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(CONFIG_DEF, originals);
    }
  }

  public ClaimCheckSourceTransform() {}

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getInt(CONFIG_THRESHOLD_BYTES);
    String storageType = ConfigUtils.getRequiredString(config, CONFIG_STORAGE_TYPE);

    this.storage = initStorage(storageType);
    this.storage.configure(configs);

    this.jsonConverter = initJsonConverter();

    log.info(
        "ClaimCheckTransform initialized. Threshold: {} bytes, Storage: {}",
        this.thresholdBytes,
        storageType);
  }

  protected ClaimCheckStorage initStorage(String type) {
    if ("S3".equalsIgnoreCase(type)) {
      return new S3Storage();
    }

    throw new ConfigException("Unsupported storage type: " + type);
  }

  private JsonConverter initJsonConverter() {
    JsonConverter converter = new JsonConverter();
    Map<String, Object> config = new HashMap<>();
    config.put("schemas.enable", "false");
    config.put("converter.type", "value");
    converter.configure(config);
    return converter;
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      return record;
    }

    byte[] valueBytes = convertValueToBytes(record);
    if (valueBytes == null) {
      return record;
    }

    if (valueBytes.length <= this.thresholdBytes) {
      return record;
    }

    String referenceUrl = storage.store(valueBytes);
    Struct referenceStruct =
        new Struct(REFERENCE_SCHEMA)
            .put("reference_url", referenceUrl)
            .put("original_size_bytes", (long) valueBytes.length)
            .put("uploaded_at", Instant.now().toEpochMilli());

    log.info(
        "Payload too large ({} bytes). Uploaded to storage: {}", valueBytes.length, referenceUrl);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        REFERENCE_SCHEMA,
        referenceStruct,
        record.timestamp());
  }

  private byte[] convertValueToBytes(SourceRecord record) {
    Object value = record.value();

    if (value == null) {
      return null;
    }

    if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof Struct || value instanceof Map) {
      return jsonConverter.fromConnectData(record.topic(), record.valueSchema(), value);
    }

    log.warn("Unsupported value type for Claim Check: {}", value.getClass().getName());
    return null;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    if (storage != null) {
      storage.close();
    }
  }
}
