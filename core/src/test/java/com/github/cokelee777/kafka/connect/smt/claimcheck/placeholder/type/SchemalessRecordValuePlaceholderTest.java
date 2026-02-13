package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SchemalessRecordValuePlaceholderTest {

  private SchemalessRecordValuePlaceholder placeholder;

  @BeforeEach
  void setUp() {
    placeholder = new SchemalessRecordValuePlaceholder();
  }

  @Nested
  class SupportsTest {

    @Test
    void shouldReturnTrueWhenSchemaIsNull() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", null, null);

      // When
      boolean supports = placeholder.supports(record);

      // Then
      assertThat(supports).isTrue();
    }

    @Test
    void shouldReturnFalseWhenSchemaIsNotNull() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.STRING_SCHEMA, "value");

      // When
      boolean supports = placeholder.supports(record);

      // Then
      assertThat(supports).isFalse();
    }
  }

  @Nested
  class ApplyTest {

    @Test
    void shouldReturnNullForSchemalessRecord() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNull();
    }
  }
}
