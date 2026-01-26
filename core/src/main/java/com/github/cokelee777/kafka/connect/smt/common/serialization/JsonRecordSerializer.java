package com.github.cokelee777.kafka.connect.smt.common.serialization;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRecordSerializer implements RecordSerializer {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordSerializer.class);

  private final JsonConverter schemaValueConverter;
  private final JsonConverter schemalessValueConverter;

  private JsonRecordSerializer(
      JsonConverter schemaValueConverter, JsonConverter schemalessValueConverter) {
    this.schemaValueConverter =
        Objects.requireNonNull(schemaValueConverter, "schemaValueConverter must not be null");
    this.schemalessValueConverter =
        Objects.requireNonNull(
            schemalessValueConverter, "schemalessValueConverter must not be null");
  }

  JsonConverter getSchemaValueConverter() {
    return schemaValueConverter;
  }

  JsonConverter getSchemalessValueConverter() {
    return schemalessValueConverter;
  }

  public static JsonRecordSerializer create() {
    // Converter for records with schema.
    JsonConverter schemaValueConverter = new JsonConverter();
    schemaValueConverter.configure(Map.of("schemas.enable", true), false);

    // Converter for schemaless records.
    JsonConverter schemalessValueConverter = new JsonConverter();
    schemalessValueConverter.configure(Map.of("schemas.enable", false), false);

    return new JsonRecordSerializer(schemaValueConverter, schemalessValueConverter);
  }

  @Override
  public String type() {
    return RecordSerializerType.JSON.type();
  }

  @Override
  public byte[] serialize(SourceRecord record) {
    Schema schema = record.valueSchema();
    if (schema != null) {
      return serializeWithSchema(record);
    }

    return serializeWithSchemaless(record);
  }

  private byte[] serializeWithSchema(SourceRecord record) {
    try {
      return schemaValueConverter.fromConnectData(
          record.topic(), record.valueSchema(), record.value());
    } catch (Exception e) {
      throw new SerializationException("Failed to serialize schema-based record value", e);
    }
  }

  private byte[] serializeWithSchemaless(SourceRecord record) {
    Object value = record.value();

    if (value instanceof byte[]) {
      return (byte[]) value;
    }

    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    if (value instanceof Map<?, ?>) {
      return serializeMap(record);
    }

    log.warn(
        "Schemaless record value of unsupported type (topic={}, type={}). Record will be skipped.",
        record.topic(),
        value != null ? value.getClass().getName() : "null");
    return null;
  }

  private byte[] serializeMap(SourceRecord record) {
    try {
      return schemalessValueConverter.fromConnectData(record.topic(), null, record.value());
    } catch (Exception e) {
      throw new SerializationException("Failed to serialize schemaless record value", e);
    }
  }

  @Override
  public SchemaAndValue deserialize(String topic, byte[] payload) {
    try {
      return schemaValueConverter.toConnectData(topic, payload);
    } catch (Exception schemaException) {
      return deserializeSchemaless(topic, payload, schemaException);
    }
  }

  private SchemaAndValue deserializeSchemaless(
      String topic, byte[] payload, Exception schemaException) {
    try {
      return schemalessValueConverter.toConnectData(topic, payload);
    } catch (Exception schemalessException) {
      log.warn(
          "Failed to deserialize record value (topic={}). Record will be skipped.",
          topic,
          schemalessException);
      log.debug("Schema-based deserialization failure", schemaException);
      return null;
    }
  }
}
