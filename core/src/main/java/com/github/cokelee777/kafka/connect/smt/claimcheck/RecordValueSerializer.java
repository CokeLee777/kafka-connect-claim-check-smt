package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Utility class for serializing and deserializing Kafka Connect record values to/from JSON bytes.
 *
 * <p>This class handles all Connect data types explicitly:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.connect.data.Struct} → JSON object
 *   <li>{@link java.util.List} → JSON array
 *   <li>{@link java.util.Map} → JSON object
 *   <li>{@code byte[]} → Base64-encoded JSON string
 *   <li>Primitives ({@code String}, {@code Integer}, {@code Long}, etc.) → JSON primitives
 * </ul>
 *
 * <p>Schema information is used for type-safe conversion during both serialization and
 * deserialization, but is not included in the serialized JSON output. This avoids storing redundant
 * schema metadata in S3, as schema information is already available via {@link
 * org.apache.kafka.connect.source.SourceRecord#valueSchema()} during SMT processing.
 *
 * <p>This class is not instantiable and all methods are static.
 *
 * <p><b>Serialization behavior by type:</b>
 *
 * <pre>
 * Schema-based:
 *   Struct   → {"field1": value1, "field2": value2, ...}
 *   Array    → [value1, value2, ...]
 *   Map      → {"key1": value1, "key2": value2, ...}
 *   byte[]   → "Base64EncodedString"
 *   others   → primitive value
 *
 * Schemaless:
 *   Map      → {"key1": value1, "key2": value2, ...}
 *   List     → [value1, value2, ...]
 *   byte[]   → "Base64EncodedString"
 *   others   → primitive value
 * </pre>
 */
public class RecordValueSerializer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private RecordValueSerializer() {}

  /**
   * Serializes a Kafka Connect {@link SourceRecord}'s value to JSON bytes.
   *
   * <p>Returns {@code null} if the record value is {@code null}. Delegates to schema-based or
   * schemaless serialization depending on whether {@link SourceRecord#valueSchema()} is present.
   *
   * @param record the source record to serialize
   * @return JSON bytes representing the record value, or {@code null} if the value is {@code null}
   * @throws SerializationException if serialization fails
   */
  public static byte[] serialize(SourceRecord record) {
    if (record.value() == null) {
      return null;
    } else if (record.valueSchema() == null) {
      return serializeSchemaless(record);
    } else {
      return serializeWithSchema(record);
    }
  }

  private static byte[] serializeSchemaless(SourceRecord record) {
    try {
      Object converted = convertSchemaless(record.value());
      return OBJECT_MAPPER.writeValueAsBytes(converted);
    } catch (JsonProcessingException e) {
      throw new SerializationException(
          "Failed to serialize schemaless record (record value=" + record.value() + ")", e);
    }
  }

  private static Object convertSchemaless(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof byte[] bytes) {
      return Base64.getEncoder().encodeToString(bytes);
    } else if (value instanceof Map<?, ?> map) {
      return convertSchemalessMap(map);
    } else if (value instanceof List<?> list) {
      return convertSchemalessList(list);
    } else {
      return value;
    }
  }

  private static Map<Object, Object> convertSchemalessMap(Map<?, ?> map) {
    Map<Object, Object> result = new LinkedHashMap<>();
    map.forEach((k, v) -> result.put(k, convertSchemaless(v)));
    return result;
  }

  private static List<Object> convertSchemalessList(List<?> list) {
    return list.stream().map(RecordValueSerializer::convertSchemaless).collect(toList());
  }

  private static byte[] serializeWithSchema(SourceRecord record) {
    try {
      Object converted = convertWithSchema(record.valueSchema(), record.value());
      return OBJECT_MAPPER.writeValueAsBytes(converted);
    } catch (JsonProcessingException e) {
      throw new SerializationException(
          "Failed to serialize schema-based record (record value=" + record.value() + ")", e);
    }
  }

  private static Object convertWithSchema(Schema schema, Object value) {
    if (value == null) {
      return null;
    }

    return switch (schema.type()) {
      case STRUCT -> convertStruct((Struct) value);
      case ARRAY -> convertArray(schema, (List<?>) value);
      case MAP -> convertMap(schema, (Map<?, ?>) value);
      case BYTES -> Base64.getEncoder().encodeToString((byte[]) value);
      default -> value;
    };
  }

  private static Map<String, Object> convertStruct(Struct struct) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Field field : struct.schema().fields()) {
      result.put(field.name(), convertWithSchema(field.schema(), struct.get(field)));
    }
    return result;
  }

  private static List<Object> convertArray(Schema schema, List<?> array) {
    return array.stream()
        .map(item -> convertWithSchema(schema.valueSchema(), item))
        .collect(toList());
  }

  private static Map<Object, Object> convertMap(Schema schema, Map<?, ?> map) {
    Map<Object, Object> result = new LinkedHashMap<>();
    map.forEach(
        (k, v) ->
            result.put(
                convertWithSchema(schema.keySchema(), k),
                convertWithSchema(schema.valueSchema(), v)));
    return result;
  }

  /**
   * Deserializes JSON bytes into a Kafka Connect value.
   *
   * <p>Returns {@code null} if {@code recordValueBytes} is {@code null}. If {@code schema} is
   * provided, the JSON is deserialized into the corresponding Connect data type (e.g., {@link
   * org.apache.kafka.connect.data.Struct}). If {@code schema} is {@code null}, the JSON is
   * deserialized into generic Java types ({@link java.util.Map}, {@link java.util.List},
   * primitives).
   *
   * @param schema the expected schema of the value, or {@code null} for schemaless deserialization
   * @param recordValueBytes the JSON bytes to deserialize
   * @return the deserialized value, or {@code null} if {@code recordValueBytes} is {@code null}
   * @throws SerializationException if deserialization fails or the JSON contains unsupported node
   *     types
   */
  public static Object deserialize(Schema schema, byte[] recordValueBytes) {
    if (recordValueBytes == null) {
      return null;
    } else if (schema == null) {
      return deserializeSchemaless(recordValueBytes);
    } else {
      return deserializeWithSchema(schema, recordValueBytes);
    }
  }

  private static Object deserializeSchemaless(byte[] recordBytes) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(recordBytes);
      return fromJsonNode(node);
    } catch (IOException e) {
      throw new SerializationException(
          "Failed to deserialize schemaless record bytes (record bytes="
              + Arrays.toString(recordBytes)
              + ")",
          e);
    }
  }

  private static Object fromJsonNode(JsonNode node) {
    return switch (node.getNodeType()) {
      case OBJECT -> fromJsonObject(node);
      case ARRAY -> fromJsonArray(node);
      case STRING -> node.asText();
      case NUMBER ->
          switch (node.numberType()) {
            case INT -> node.asInt();
            case LONG -> node.asLong();
            case BIG_INTEGER -> node.bigIntegerValue();
            case FLOAT -> node.floatValue();
            case DOUBLE -> node.asDouble();
            case BIG_DECIMAL -> node.decimalValue();
          };
      case BOOLEAN -> node.asBoolean();
      case NULL, MISSING ->
          throw new SerializationException(
              "Cannot deserialize null or missing JSON node. "
                  + "The serialized data may be corrupted or in an unsupported format.");
      default ->
          throw new SerializationException(
              "Unsupported JSON node type during deserialization: "
                  + node.getNodeType()
                  + ". "
                  + "Only OBJECT, ARRAY, BINARY, STRING, NUMBER, and BOOLEAN are supported.");
    };
  }

  private static Map<String, Object> fromJsonObject(JsonNode node) {
    Map<String, Object> result = new LinkedHashMap<>();
    node.fields()
        .forEachRemaining(entry -> result.put(entry.getKey(), fromJsonNode(entry.getValue())));
    return result;
  }

  private static List<Object> fromJsonArray(JsonNode node) {
    List<Object> result = new ArrayList<>();
    node.elements().forEachRemaining(element -> result.add(fromJsonNode(element)));
    return result;
  }

  private static Object deserializeWithSchema(Schema schema, byte[] recordBytes) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(recordBytes);
      return fromJsonNodeWithSchema(schema, node);
    } catch (IOException e) {
      throw new SerializationException(
          "Failed to deserialize schema-based record bytes (record bytes="
              + Arrays.toString(recordBytes)
              + ")",
          e);
    }
  }

  private static Object fromJsonNodeWithSchema(Schema schema, JsonNode node) {
    if (node.isNull()) {
      return null;
    }

    return switch (schema.type()) {
      case STRUCT -> fromJsonNodeAsStruct(schema, node);
      case ARRAY -> fromJsonNodeAsArray(schema, node);
      case MAP -> fromJsonNodeAsMap(schema, node);
      case BYTES -> Base64.getDecoder().decode(node.asText());
      case INT8 -> (byte) node.asInt();
      case INT16 -> (short) node.asInt();
      case INT32 -> node.asInt();
      case INT64 -> node.asLong();
      case FLOAT32 -> (float) node.asDouble();
      case FLOAT64 -> node.asDouble();
      case BOOLEAN -> node.asBoolean();
      case STRING -> node.asText();
    };
  }

  private static Struct fromJsonNodeAsStruct(Schema schema, JsonNode node) {
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      JsonNode fieldNode = node.get(field.name());
      struct.put(field, fromJsonNodeWithSchema(field.schema(), fieldNode));
    }
    return struct;
  }

  private static List<Object> fromJsonNodeAsArray(Schema schema, JsonNode node) {
    List<Object> result = new ArrayList<>();
    node.elements()
        .forEachRemaining(
            element -> result.add(fromJsonNodeWithSchema(schema.valueSchema(), element)));
    return result;
  }

  private static Map<Object, Object> fromJsonNodeAsMap(Schema schema, JsonNode node) {
    Map<Object, Object> result = new LinkedHashMap<>();
    node.fields()
        .forEachRemaining(
            entry ->
                result.put(
                    fromJsonNodeWithSchema(schema.keySchema(), new TextNode(entry.getKey())),
                    fromJsonNodeWithSchema(schema.valueSchema(), entry.getValue())));
    return result;
  }
}
