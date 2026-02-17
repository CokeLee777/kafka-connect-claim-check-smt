package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RecordValueSerializerTest {

  @Nested
  class SerializeTest {

    @Test
    void shouldSerializeStructWithSchema() {
      // Given
      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      Struct value = new Struct(schema).put("id", 1L).put("name", "cokelee777");
      SourceRecord record = new SourceRecord(null, null, "test-topic", schema, value);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result)
          .isEqualTo("{\"id\":1,\"name\":\"cokelee777\"}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeNestedStructWithSchema() {
      // Given
      Schema addressSchema =
          SchemaBuilder.struct()
              .field("city", Schema.STRING_SCHEMA)
              .field("zipcode", Schema.STRING_SCHEMA)
              .build();
      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("address", addressSchema)
              .build();
      Struct address = new Struct(addressSchema).put("city", "Seoul").put("zipcode", "12345");
      Struct value = new Struct(schema).put("id", 1L).put("address", address);
      SourceRecord record = new SourceRecord(null, null, "test-topic", schema, value);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result)
          .isEqualTo(
              "{\"id\":1,\"address\":{\"city\":\"Seoul\",\"zipcode\":\"12345\"}}"
                  .getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeArrayWithSchema() {
      // Given
      Schema schema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
      List<String> value = List.of("a", "b", "c");
      SourceRecord record = new SourceRecord(null, null, "test-topic", schema, value);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo("[\"a\",\"b\",\"c\"]".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeMapWithSchema() {
      // Given
      Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
      Map<String, Integer> value = new LinkedHashMap<>();
      value.put("a", 1);
      value.put("b", 2);
      SourceRecord record = new SourceRecord(null, null, "test-topic", schema, value);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo("{\"a\":1,\"b\":2}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeBytesWithSchemaAsBase64() {
      // Given
      byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, bytes);
      String expectedBase64 = "\"" + Base64.getEncoder().encodeToString(bytes) + "\"";

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo(expectedBase64.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializePrimitiveStringWithSchema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.STRING_SCHEMA, "hello");

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo("\"hello\"".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializePrimitiveIntWithSchema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.INT32_SCHEMA, 42);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo("42".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeSchemalessMap() {
      // Given
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("id", 1);
      value.put("name", "cokelee777");
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, value);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result)
          .isEqualTo("{\"id\":1,\"name\":\"cokelee777\"}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeSchemalessList() {
      // Given
      List<Object> value = List.of("a", 1, true);
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, value);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo("[\"a\",1,true]".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeSchemalessBytesAsBase64() {
      // Given
      byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, bytes);
      String expectedBase64 = "\"" + Base64.getEncoder().encodeToString(bytes) + "\"";

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo(expectedBase64.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSerializeSchemalessString() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, "hello");

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isEqualTo("\"hello\"".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldReturnNullWhenRecordValueIsNull() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, null);

      // When
      byte[] result = RecordValueSerializer.serialize(record);

      // Then
      assertThat(result).isNull();
    }
  }

  @Nested
  class DeserializeTest {

    @Test
    void shouldDeserializeStructWithSchema() {
      // Given
      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      byte[] recordBytes = "{\"id\":1,\"name\":\"cokelee777\"}".getBytes(StandardCharsets.UTF_8);
      Struct expectedValue = new Struct(schema).put("id", 1L).put("name", "cokelee777");

      // When
      Object result = RecordValueSerializer.deserialize(schema, recordBytes);

      // Then
      assertThat(result).isEqualTo(expectedValue);
    }

    @Test
    void shouldDeserializeNestedStructWithSchema() {
      // Given
      Schema addressSchema =
          SchemaBuilder.struct()
              .field("city", Schema.STRING_SCHEMA)
              .field("zipcode", Schema.STRING_SCHEMA)
              .build();
      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("address", addressSchema)
              .build();
      byte[] recordBytes =
          "{\"id\":1,\"address\":{\"city\":\"Seoul\",\"zipcode\":\"12345\"}}"
              .getBytes(StandardCharsets.UTF_8);
      Struct address = new Struct(addressSchema).put("city", "Seoul").put("zipcode", "12345");
      Struct expectedValue = new Struct(schema).put("id", 1L).put("address", address);

      // When
      Object result = RecordValueSerializer.deserialize(schema, recordBytes);

      // Then
      assertThat(result).isEqualTo(expectedValue);
    }

    @Test
    void shouldDeserializeArrayWithSchema() {
      // Given
      Schema schema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
      byte[] recordBytes = "[\"a\",\"b\",\"c\"]".getBytes(StandardCharsets.UTF_8);

      // When
      Object result = RecordValueSerializer.deserialize(schema, recordBytes);

      // Then
      assertThat(result).isEqualTo(List.of("a", "b", "c"));
    }

    @Test
    void shouldDeserializeMapWithSchema() {
      // Given
      Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
      byte[] recordBytes = "{\"a\":1,\"b\":2}".getBytes(StandardCharsets.UTF_8);
      Map<Object, Object> expectedValue = new LinkedHashMap<>();
      expectedValue.put("a", 1);
      expectedValue.put("b", 2);

      // When
      Object result = RecordValueSerializer.deserialize(schema, recordBytes);

      // Then
      assertThat(result).isEqualTo(expectedValue);
    }

    @Test
    void shouldDeserializeBytesWithSchemaFromBase64() {
      // Given
      byte[] originalBytes = "hello".getBytes(StandardCharsets.UTF_8);
      String base64 = "\"" + Base64.getEncoder().encodeToString(originalBytes) + "\"";
      byte[] recordBytes = base64.getBytes(StandardCharsets.UTF_8);

      // When
      Object result = RecordValueSerializer.deserialize(Schema.BYTES_SCHEMA, recordBytes);

      // Then
      assertThat(result).isEqualTo(originalBytes);
    }

    @Test
    void shouldDeserializePrimitiveStringWithSchema() {
      // Given
      byte[] recordBytes = "\"hello\"".getBytes(StandardCharsets.UTF_8);

      // When
      Object result = RecordValueSerializer.deserialize(Schema.STRING_SCHEMA, recordBytes);

      // Then
      assertThat(result).isEqualTo("hello");
    }

    @Test
    void shouldDeserializePrimitiveIntWithSchema() {
      // Given
      byte[] recordBytes = "42".getBytes(StandardCharsets.UTF_8);

      // When
      Object result = RecordValueSerializer.deserialize(Schema.INT32_SCHEMA, recordBytes);

      // Then
      assertThat(result).isEqualTo(42);
    }

    @Test
    void shouldDeserializeSchemalessMap() {
      // Given
      byte[] recordBytes = "{\"id\":1,\"name\":\"cokelee777\"}".getBytes(StandardCharsets.UTF_8);
      Map<String, Object> expectedValue = new LinkedHashMap<>();
      expectedValue.put("id", 1);
      expectedValue.put("name", "cokelee777");

      // When
      Object result = RecordValueSerializer.deserialize(null, recordBytes);

      // Then
      assertThat(result).isEqualTo(expectedValue);
    }

    @Test
    void shouldDeserializeSchemalessList() {
      // Given
      byte[] recordBytes = "[\"a\",1,true]".getBytes(StandardCharsets.UTF_8);

      // When
      Object result = RecordValueSerializer.deserialize(null, recordBytes);

      // Then
      assertThat(result).isEqualTo(List.of("a", 1, true));
    }

    @Test
    void shouldReturnNullWhenRecordBytesIsNull() {
      // Null input is a valid contract: callers may pass null when no bytes are available.
      // Given
      byte[] recordValueBytes = null;

      // When
      Object result = RecordValueSerializer.deserialize(Schema.STRING_SCHEMA, recordValueBytes);

      // Then
      assertThat(result).isNull();
    }

    @Test
    void shouldThrowWhenDeserializingInvalidJson() {
      // Given
      byte[] invalidBytes = "not-valid-json{{".getBytes(StandardCharsets.UTF_8);

      // When & Then
      assertThatThrownBy(() -> RecordValueSerializer.deserialize(null, invalidBytes))
          .isInstanceOf(SerializationException.class);
    }

    @Test
    void shouldThrowWhenDeserializingInvalidJsonWithSchema() {
      // Given
      byte[] invalidBytes = "not-valid-json{{".getBytes(StandardCharsets.UTF_8);

      // When & Then
      assertThatThrownBy(
              () -> RecordValueSerializer.deserialize(Schema.STRING_SCHEMA, invalidBytes))
          .isInstanceOf(SerializationException.class);
    }
  }
}
