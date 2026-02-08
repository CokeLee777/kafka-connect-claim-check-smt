package com.github.cokelee777.kafka.connect.smt.common.serialization;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class RecordSerializerFactoryTest {

  @Nested
  class CreateTest {

    @Test
    void shouldCreateJsonRecordSerializer() {
      // Given
      String type = RecordSerializerType.JSON.type();

      // When
      RecordSerializer recordSerializer = RecordSerializerFactory.create(type);

      // Then
      assertThat(recordSerializer).isNotNull();
      assertThat(recordSerializer).isInstanceOf(JsonRecordSerializer.class);
      assertThat(recordSerializer.type()).isEqualTo(type);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void shouldCreateDefaultSerializerWhenTypeIsBlank(String type) {
      // When
      RecordSerializer recordSerializer = RecordSerializerFactory.create(type);

      // Then
      assertThat(recordSerializer).isNotNull();
      assertThat(recordSerializer).isInstanceOf(JsonRecordSerializer.class);
      assertThat(recordSerializer.type()).isEqualTo(RecordSerializerType.JSON.type());
    }

    @ParameterizedTest
    @ValueSource(strings = {"unsupportedType"})
    void shouldThrowExceptionWhenSerializerTypeIsUnsupported(String type) {
      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> RecordSerializerFactory.create(type))
          .withMessage("Unsupported serializer type: " + type);
    }
  }
}
