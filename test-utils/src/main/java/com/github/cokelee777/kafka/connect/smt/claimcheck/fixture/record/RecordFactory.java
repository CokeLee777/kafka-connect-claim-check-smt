package com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.record;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Utility class for creating Kafka Connect {@link SourceRecord} and {@link SinkRecord} instances in
 * tests.
 */
public class RecordFactory {

  private RecordFactory() {}

  /**
   * Creates a {@link SourceRecord} with a struct value built from the given field definitions.
   *
   * <pre>{@code
   * SourceRecord record = RecordFactory.sourceRecord(
   *     "my-topic",
   *     Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA)
   * );
   * }</pre>
   *
   * @param topic destination topic name
   * @param fields ordered map of fieldName → Schema (insertion order preserved)
   * @return a {@link SourceRecord} whose value is a {@link Struct} with each field set to its
   *     default value
   */
  public static SourceRecord sourceRecord(String topic, Map<String, Schema> fields) {
    return sourceRecord(topic, fields, Map.of());
  }

  /**
   * Creates a {@link SourceRecord} with a struct value populated with the given field values.
   *
   * <pre>{@code
   * SourceRecord record = RecordFactory.sourceRecord(
   *     "my-topic",
   *     Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
   *     Map.of("id", 1L, "name", "cokelee777")
   * );
   * }</pre>
   *
   * @param topic destination topic name
   * @param fields ordered map of fieldName → Schema
   * @param fieldValues map of fieldName → value; fields absent from this map are set to their
   *     schema default
   * @return a populated {@link SourceRecord}
   */
  public static SourceRecord sourceRecord(
      String topic, Map<String, Schema> fields, Map<String, Object> fieldValues) {

    Schema schema = buildSchema(fields);
    Struct value = buildStruct(schema, fields, fieldValues);
    return new SourceRecord(null, null, topic, null, null, schema, value);
  }

  /**
   * Creates a {@link SourceRecord} with explicit key schema/value in addition to the struct value.
   */
  public static SourceRecord sourceRecord(
      String topic,
      Schema keySchema,
      Object key,
      Map<String, Schema> fields,
      Map<String, Object> fieldValues) {

    Schema valueSchema = buildSchema(fields);
    Struct value = buildStruct(valueSchema, fields, fieldValues);
    return new SourceRecord(null, null, topic, null, keySchema, key, valueSchema, value);
  }

  /**
   * Creates a schemaless {@link SourceRecord} whose value is the given {@link Map}.
   *
   * <pre>{@code
   * SourceRecord record = RecordFactory.schemalessSourceRecord(
   *     "my-topic",
   *     Map.of("id", 1L, "name", "cokelee777")
   * );
   * }</pre>
   *
   * @param topic destination topic name
   * @param value schemaless value as a plain {@link Map}
   * @return a schemaless {@link SourceRecord} with {@code null} key and {@code null} schemas
   */
  public static SourceRecord schemalessSourceRecord(String topic, Map<String, Object> value) {
    return new SourceRecord(null, null, topic, null, null, null, null, value);
  }

  /**
   * Creates a schemaless {@link SourceRecord} with an explicit key.
   *
   * @param topic destination topic name
   * @param key schemaless key (e.g. a plain {@link String} or {@link Map})
   * @param value schemaless value as a plain {@link Map}
   * @return a schemaless {@link SourceRecord} with {@code null} schemas
   */
  public static SourceRecord schemalessSourceRecord(
      String topic, Object key, Map<String, Object> value) {
    return new SourceRecord(null, null, topic, null, null, key, null, value);
  }

  /**
   * Creates a {@link SinkRecord} with a struct value built from the given field definitions.
   *
   * <pre>{@code
   * SinkRecord record = RecordFactory.sinkRecord(
   *     "my-topic",
   *     Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA)
   * );
   * }</pre>
   *
   * @param topic destination topic name
   * @param fields ordered map of fieldName → Schema
   * @return a {@link SinkRecord} whose value is a {@link Struct} with each field set to its default
   *     value
   */
  public static SinkRecord sinkRecord(String topic, Map<String, Schema> fields) {
    return sinkRecord(topic, fields, Map.of());
  }

  /**
   * Creates a {@link SinkRecord} with a struct value populated with the given field values.
   *
   * <pre>{@code
   * SinkRecord record = RecordFactory.sinkRecord(
   *     "my-topic",
   *     Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
   *     Map.of("id", 1L, "name", "cokelee777")
   * );
   * }</pre>
   *
   * @param topic destination topic name
   * @param fields ordered map of fieldName → Schema
   * @param fieldValues map of fieldName → value; fields absent from this map are set to their
   *     schema default
   * @return a populated {@link SinkRecord}
   */
  public static SinkRecord sinkRecord(
      String topic, Map<String, Schema> fields, Map<String, Object> fieldValues) {

    Schema schema = buildSchema(fields);
    Struct value = buildStruct(schema, fields, fieldValues);
    return new SinkRecord(topic, 0, null, null, schema, value, 0L);
  }

  /**
   * Creates a {@link SinkRecord} with explicit key schema/value in addition to the struct value.
   *
   * @param topic destination topic name
   * @param keySchema schema of the key
   * @param key key value
   * @param fields ordered map of fieldName → Schema
   * @param fieldValues map of fieldName → value
   * @return a populated {@link SinkRecord} with key
   */
  public static SinkRecord sinkRecord(
      String topic,
      Schema keySchema,
      Object key,
      Map<String, Schema> fields,
      Map<String, Object> fieldValues) {

    Schema valueSchema = buildSchema(fields);
    Struct value = buildStruct(valueSchema, fields, fieldValues);
    return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 0L);
  }

  /**
   * Creates a {@link SinkRecord} that mirrors a {@link SourceRecord} — the typical pattern used
   * when a source transform produces a record that a sink transform later consumes.
   *
   * <p>Partition defaults to {@code 0} and offset defaults to {@code 0}.
   *
   * @param sourceRecord the source record to mirror
   * @return a {@link SinkRecord} with the same topic / key / value schemas and values
   */
  public static SinkRecord sinkRecord(SourceRecord sourceRecord) {
    return sinkRecord(sourceRecord, 0, 0L);
  }

  /**
   * Creates a {@link SinkRecord} that mirrors a {@link SourceRecord} with explicit partition and
   * offset.
   */
  public static SinkRecord sinkRecord(SourceRecord sourceRecord, int partition, long offset) {
    return new SinkRecord(
        sourceRecord.topic(),
        partition,
        sourceRecord.keySchema(),
        sourceRecord.key(),
        sourceRecord.valueSchema(),
        sourceRecord.value(),
        offset);
  }

  /**
   * Creates a {@link SinkRecord} that mirrors a {@link SourceRecord} and appends a single header.
   *
   * <p>This is the most common usage in claim-check flows: the source transform emits a header that
   * must be forwarded to the sink transform.
   *
   * @param sourceRecord source record to mirror
   * @param header header to attach (e.g. the ClaimCheck metadata header)
   * @return a {@link SinkRecord} with the header attached
   */
  public static SinkRecord sinkRecord(SourceRecord sourceRecord, Header header) {
    return sinkRecord(sourceRecord, header, 0, 0L);
  }

  /**
   * Creates a {@link SinkRecord} that mirrors a {@link SourceRecord}, appends a single header, and
   * uses explicit partition and offset values.
   */
  public static SinkRecord sinkRecord(
      SourceRecord sourceRecord, Header header, int partition, long offset) {

    SinkRecord sinkRecord = sinkRecord(sourceRecord, partition, offset);
    sinkRecord.headers().add(header);
    return sinkRecord;
  }

  /**
   * Creates a schemaless {@link SinkRecord} with the given value map.
   *
   * <pre>{@code
   * SinkRecord record = RecordFactory.schemalessSinkRecord(
   *     "my-topic",
   *     Map.of("id", 1L, "name", "cokelee777")
   * );
   * }</pre>
   *
   * @param topic destination topic name
   * @param value schemaless value as a plain {@link Map}
   * @return a schemaless {@link SinkRecord} with {@code null} schemas, partition {@code 0}, offset
   *     {@code 0}
   */
  public static SinkRecord schemalessSinkRecord(String topic, Map<String, Object> value) {
    return schemalessSinkRecord(topic, null, value, 0, 0L);
  }

  /**
   * Creates a schemaless {@link SinkRecord} with an explicit key.
   *
   * @param topic destination topic name
   * @param key schemaless key (e.g. a plain {@link String} or {@link Map})
   * @param value schemaless value as a plain {@link Map}
   * @return a schemaless {@link SinkRecord} with {@code null} schemas, partition {@code 0}, offset
   *     {@code 0}
   */
  public static SinkRecord schemalessSinkRecord(
      String topic, Object key, Map<String, Object> value) {
    return schemalessSinkRecord(topic, key, value, 0, 0L);
  }

  /**
   * Creates a schemaless {@link SinkRecord} with explicit partition and offset.
   *
   * @param topic destination topic name
   * @param key schemaless key; pass {@code null} for no key
   * @param value schemaless value as a plain {@link Map}
   * @param partition partition number
   * @param offset record offset
   * @return a fully configured schemaless {@link SinkRecord}
   */
  public static SinkRecord schemalessSinkRecord(
      String topic, Object key, Map<String, Object> value, int partition, long offset) {
    return new SinkRecord(topic, partition, null, key, null, value, offset);
  }

  private static Schema buildSchema(Map<String, Schema> fields) {
    SchemaBuilder builder = SchemaBuilder.struct();
    fields.forEach(builder::field);
    return builder.build();
  }

  /**
   * Populates a {@link Struct} with values from {@code fieldValues}. Fields not present in {@code
   * fieldValues} are set to a sensible default:
   *
   * <ul>
   *   <li>STRING → {@code ""}
   *   <li>INT8/INT16/INT32 → {@code 0}
   *   <li>INT64 → {@code 0L}
   *   <li>FLOAT32 → {@code 0.0f}
   *   <li>FLOAT64 → {@code 0.0d}
   *   <li>BOOLEAN → {@code false}
   *   <li>BYTES → {@code new byte[0]}
   *   <li>other / null → {@code null}
   * </ul>
   */
  private static Struct buildStruct(
      Schema schema, Map<String, Schema> fields, Map<String, Object> fieldValues) {

    Struct struct = new Struct(schema);
    fields.forEach(
        (name, fieldSchema) -> {
          Object value =
              fieldValues.containsKey(name) ? fieldValues.get(name) : defaultValue(fieldSchema);
          struct.put(name, value);
        });
    return struct;
  }

  private static Object defaultValue(Schema schema) {
    if (schema == null) return null;
    return switch (schema.type()) {
      case STRING -> "";
      case INT8 -> (byte) 0;
      case INT16 -> (short) 0;
      case INT32 -> 0;
      case INT64 -> 0L;
      case FLOAT32 -> 0.0f;
      case FLOAT64 -> 0.0d;
      case BOOLEAN -> false;
      case BYTES -> new byte[0];
      default -> null;
    };
  }
}
