package connect.smt.claimcheck.source;

import connect.smt.claimcheck.storage.ClaimCheckStorage;
import connect.smt.claimcheck.storage.S3Storage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);
  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneId.of("UTC"));

  // Config 상수 정의
  public static final String CONFIG_STORAGE_TYPE = "storage.type";
  public static final String CONFIG_THRESHOLD_BYTES = "threshold.bytes";

  // 기본값 설정 (1MB)
  private static final int DEFAULT_THRESHOLD = 1024 * 1024;
  private static final String DEFAULT_STORAGE_TYPE = "S3";

  private ClaimCheckStorage storage;
  private int thresholdBytes;

  // ConfigDef (설정 정의)
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

  public ClaimCheckSourceTransform() {}

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

    String storageType = config.getString(CONFIG_STORAGE_TYPE);
    this.thresholdBytes = config.getInt(CONFIG_THRESHOLD_BYTES);

    if ("S3".equalsIgnoreCase(storageType)) {
      this.storage = new S3Storage();
    } else {
      throw new ConfigException("Unsupported storage type: " + storageType);
    }

    this.storage.configure(configs);

    log.info(
        "ClaimCheckTransform initialized. Threshold: {} bytes, Storage: {}",
        this.thresholdBytes,
        storageType);
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      return record;
    }

    byte[] valueBytes = getBytesFromValue(record.value());
    if (valueBytes == null) {
      return record;
    }

    if (valueBytes.length <= thresholdBytes) {
      return record;
    }

    String key = generateObjectKey(record);
    String referenceUrl = storage.store(key, valueBytes);

    log.info(
        "Payload too large ({} bytes). Uploaded to storage: {}", valueBytes.length, referenceUrl);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema() != null ? Schema.STRING_SCHEMA : null,
        referenceUrl,
        record.timestamp());
  }

  private byte[] getBytesFromValue(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    log.warn("Unsupported value type for Claim Check: {}", value.getClass().getName());
    return null;
  }

  private String generateObjectKey(SourceRecord record) {
    Long timestamp = record.timestamp();
    Instant instant;

    if (timestamp == null) {
      instant = Instant.now();
    } else {
      instant = Instant.ofEpochMilli(timestamp);
    }

    String datePath = DATE_FORMATTER.format(instant);
    String extension = resolveExtension(record);
    return "topics/" + record.topic() + "/" + datePath + "/" + UUID.randomUUID() + "." + extension;
  }

  private String resolveExtension(SourceRecord record) {
    if (record.valueSchema() == null) {
      return "bin";
    }

    String schemaName = record.valueSchema().name();
    if (schemaName == null) {
      return "bin";
    }

    if (schemaName.contains("avro")) return "avro";
    if (schemaName.contains("json")) return "json";

    return "bin";
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
