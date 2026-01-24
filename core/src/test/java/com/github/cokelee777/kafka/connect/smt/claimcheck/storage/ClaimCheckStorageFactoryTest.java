package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("ClaimCheckStorageFactory 단위 테스트")
public class ClaimCheckStorageFactoryTest {

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("올바른 Type을 인자로 넘겨주면 ClaimCheckStorage 객체를 생성한다.")
    public void rightType() {
      // Given
      String type = ClaimCheckStorageType.S3.type();

      // When
      ClaimCheckStorage claimCheckStorage = ClaimCheckStorageFactory.create(type);

      // Then
      assertThat(claimCheckStorage).isNotNull();
      assertThat(claimCheckStorage).isInstanceOf(S3Storage.class);
      assertThat(claimCheckStorage.type()).isEqualTo(type);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalidType"})
    @DisplayName("지원하지 않는 Type을 인자로 넘기면 예외가 발생한다.")
    public void invalidTypeCauseException(String type) { // Given
      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> ClaimCheckStorageFactory.create(type))
          .withMessage("Unsupported storage type: " + type);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    @DisplayName("null 또는 빈 값의 Type을 인자로 넘기면 예외가 발생한다.")
    public void nullOrBlankTypeCauseException(String type) { // Given
      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> ClaimCheckStorageFactory.create(type))
          .withMessage("Storage type must be provided");
    }
  }
}
