package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("RecordSerializerFactory 단위 테스트")
class RecordSerializerFactoryTest {

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("올바른 Type을 인자로 넘겨주면 RecordSerializer 객체를 생성한다.")
    public void rightType() {
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
    @DisplayName("null 또는 빈 값의 Type을 인자로 넘기면 기본 Type으로 객체를 생성한다.")
    public void nullOrBlankTypeCreateDefaultType(String type) { // Given
      // When
      RecordSerializer recordSerializer = RecordSerializerFactory.create(type);

      // Then
      assertThat(recordSerializer).isNotNull();
      assertThat(recordSerializer).isInstanceOf(JsonRecordSerializer.class);
      assertThat(recordSerializer.type()).isEqualTo(RecordSerializerType.JSON.type());
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalidType"})
    @DisplayName("지원하지 않는 Type을 인자로 넘기면 예외가 발생한다.")
    public void invalidTypeCauseException(String type) { // Given
      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> RecordSerializerFactory.create(type))
          .withMessage("Unsupported serializer type: " + type);
    }
  }
}
