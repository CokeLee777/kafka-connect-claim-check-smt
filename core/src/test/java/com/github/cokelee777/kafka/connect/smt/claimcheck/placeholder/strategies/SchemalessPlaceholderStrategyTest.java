package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("SchemalessPlaceholderStrategy 단위 테스트")
class SchemalessPlaceholderStrategyTest {

  private SchemalessPlaceholderStrategy schemalessPlaceholderStrategy;

  @BeforeEach
  void beforeEach() {
    schemalessPlaceholderStrategy = new SchemalessPlaceholderStrategy();
  }

  @Nested
  @DisplayName("apply 메서드 테스트")
  class ApplyTest {

    @Test
    @DisplayName("처리할 수 있는 Record를 인자로 넣으면 null이 반환된다.")
    void rightArgsReturnNull() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      Object defaultValue = schemalessPlaceholderStrategy.apply(record);

      // Then
      assertThat(defaultValue).isNull();
    }

    @Test
    @DisplayName("처리할 수 없는 Record를 인자로 넣으면 예외가 발생한다.")
    void wrongArgsCauseException() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", Schema.STRING_SCHEMA, value);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> schemalessPlaceholderStrategy.apply(record))
          .withMessage("Cannot handle record with non-null schema. Expected schemaless record.");
    }
  }
}
