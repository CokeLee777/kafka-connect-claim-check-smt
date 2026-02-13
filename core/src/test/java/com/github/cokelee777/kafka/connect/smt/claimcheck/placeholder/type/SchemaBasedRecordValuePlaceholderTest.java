package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SchemaBasedRecordValuePlaceholderTest {

  private SchemaBasedRecordValuePlaceholder placeholder;

  @BeforeEach
  void setUp() {
    placeholder = new SchemaBasedRecordValuePlaceholder();
  }

  @Nested
  class SupportsTest {

    @Test
    void shouldReturnFalseWhenSchemaIsNull() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, null);

      // When
      boolean supports = placeholder.supports(record);

      // Then
      assertThat(supports).isFalse();
    }

    @Test
    void shouldReturnTrueWhenSchemaIsNotNull() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.STRING_SCHEMA, "value");

      // When
      boolean supports = placeholder.supports(record);

      // Then
      assertThat(supports).isTrue();
    }
  }

  @Nested
  class ApplyTest {

    @Test
    void shouldThrowNullPointerExceptionWhenSchemaIsNull() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, null);

      // When & Then
      assertThatThrownBy(() -> placeholder.apply(record)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldReturnDefaultValueWhenSchemaHasDefaultValue() {
      // Given
      Schema valueSchema = SchemaBuilder.string().defaultValue("default_string").build();
      SourceRecord record = new SourceRecord(null, null, "test-topic", valueSchema, "original");

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isEqualTo("default_string");
    }

    @Test
    void shouldReturnNullWhenSchemaIsOptional() {
      // Given
      Schema valueSchema = SchemaBuilder.string().optional().build();
      SourceRecord record = new SourceRecord(null, null, "test-topic", valueSchema, "original");

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNull();
    }

    @Test
    void shouldReturnDefaultValuesForAllFieldsForStructSchema() {
      // Given
      Schema nestedSchema =
          SchemaBuilder.struct()
              .name("nestedPayload")
              .field("nestedId", Schema.INT64_SCHEMA)
              .field("nestedName", Schema.STRING_SCHEMA)
              .build();
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .field("nestedPayload", nestedSchema)
              .build();
      Struct nestedValue =
          new Struct(nestedSchema).put("nestedId", 1L).put("nestedName", "nested cokelee777");
      Struct value =
          new Struct(valueSchema)
              .put("id", 1L)
              .put("name", "cokelee777")
              .put("nestedPayload", nestedValue);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNotNull();
      assertThat(defaultValue).isInstanceOf(Struct.class);
      assertThat(((Struct) defaultValue).getInt64("id")).isEqualTo(0L);
      assertThat(((Struct) defaultValue).getString("name")).isEqualTo("");
      assertThat(((Struct) defaultValue).getStruct("nestedPayload").getInt64("nestedId"))
          .isEqualTo(0L);
      assertThat(((Struct) defaultValue).getStruct("nestedPayload").getString("nestedName"))
          .isEqualTo("");
    }

    @Test
    void shouldReturnEmptyMapForMapSchema() {
      // Given
      Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", valueSchema, Collections.singletonMap("key", 1));

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNotNull();
      assertThat(defaultValue).isInstanceOf(Map.class);
      assertThat((Map<?, ?>) defaultValue).isEmpty();
    }

    @Test
    void shouldReturnEmptyListForArraySchema() {
      // Given
      Schema valueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", valueSchema, Collections.singletonList("item"));

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNotNull();
      assertThat(defaultValue).isInstanceOf(List.class);
      assertThat((List<?>) defaultValue).isEmpty();
    }

    @Test
    void shouldReturnDefaultValueForInt8Schema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.INT8_SCHEMA, (byte) 1);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo((byte) 0);
    }

    @Test
    void shouldReturnDefaultValueForInt16Schema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.INT16_SCHEMA, (short) 1);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo((short) 0);
    }

    @Test
    void shouldReturnDefaultValueForInt32Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.INT32_SCHEMA, 1);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(0);
    }

    @Test
    void shouldReturnDefaultValueForInt64Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.INT64_SCHEMA, 1L);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(0L);
    }

    @Test
    void shouldReturnDefaultValueForFloat32Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.FLOAT32_SCHEMA, 1.0f);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(0.0f);
    }

    @Test
    void shouldReturnDefaultValueForFloat64Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.FLOAT64_SCHEMA, 1.0);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(0.0);
    }

    @Test
    void shouldReturnDefaultValueForBooleanSchema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.BOOLEAN_SCHEMA, true);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(false);
    }

    @Test
    void shouldReturnDefaultValueForStringSchema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.STRING_SCHEMA, "hello");
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo("");
    }

    @Test
    void shouldReturnDefaultValueForBytesSchema() {
      // Given
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[] {1}));
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(ByteBuffer.wrap(new byte[0]));
    }

    @Test
    void shouldReturnDefaultValueForTimestampSchema() {
      // Given
      Schema timestampSchema = Timestamp.SCHEMA;
      SourceRecord record = new SourceRecord(null, null, "test-topic", timestampSchema, new Date());
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(new Date(0));
    }

    @Test
    void shouldReturnDefaultValueForDecimalSchema() {
      // Given
      Schema decimalSchema = Decimal.schema(2);
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", decimalSchema, BigDecimal.ONE);
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    void shouldReturnDefaultValueForDateSchema() {
      // Given
      Schema dateSchema = org.apache.kafka.connect.data.Date.SCHEMA;
      SourceRecord record = new SourceRecord(null, null, "test-topic", dateSchema, new Date());
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(new Date(0));
    }

    @Test
    void shouldReturnDefaultValueForTimeSchema() {
      // Given
      Schema timeSchema = Time.SCHEMA;
      SourceRecord record = new SourceRecord(null, null, "test-topic", timeSchema, new Date());
      // When
      Object defaultValue = placeholder.apply(record);
      // Then
      assertThat(defaultValue).isEqualTo(new Date(0));
    }
  }
}
