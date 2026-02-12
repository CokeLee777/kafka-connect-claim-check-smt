package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DebeziumStructRecordValuePlaceholderTest {

  private DebeziumStructRecordValuePlaceholder placeholder;

  @BeforeEach
  void setUp() {
    placeholder = new DebeziumStructRecordValuePlaceholder();
  }

  @Nested
  class ApplyTest {

    @Test
    void shouldReturnDefaultValuesExceptMetadata() {
      // Given
      Schema nestedSchema =
          SchemaBuilder.struct()
              .name("test.db.table.Value")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .optional()
              .build();
      Schema envelopeSchema =
          SchemaBuilder.struct()
              .name("io.debezium.connector.mysql.Envelope")
              .field("before", nestedSchema)
              .field("after", nestedSchema)
              .field("op", Schema.STRING_SCHEMA)
              .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
              .build();
      Struct before = new Struct(nestedSchema).put("id", 1L).put("name", "before cokelee777");
      Struct after = new Struct(nestedSchema).put("id", 1L).put("name", "after cokelee777");
      long tsMs = System.currentTimeMillis();
      Struct envelope =
          new Struct(envelopeSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", tsMs);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", envelopeSchema, envelope);

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNotNull();
      assertThat(defaultValue).isInstanceOf(Struct.class);
      assertThat(((Struct) defaultValue).getStruct("before")).isNull();
      assertThat(((Struct) defaultValue).getStruct("after")).isNull();
      assertThat(((Struct) defaultValue).getString("op")).isEqualTo("c");
      assertThat(((Struct) defaultValue).getInt64("ts_ms")).isEqualTo(tsMs);
    }

    @Test
    void shouldThrowExceptionWhenRecordSchemaIsNotDebezium() {
      // Given
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      Struct value = new Struct(valueSchema).put("id", 1L).put("name", "cokelee777");
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> placeholder.apply(record))
          .withMessageContaining(
              "Cannot handle record. Expected Debezium STRUCT schema for placeholder processing.");
    }
  }
}
