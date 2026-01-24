package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("ClaimCheckReference 단위 테스트")
class ClaimCheckValueTest {

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("올바른 인자를 넘기면 정상적으로 객체가 생성된다.")
    void rightArgs() {
      // Given
      String referenceUrl = "s3://test-bucket/claim-checks";
      long originalSizeBytes = 1024 * 1024;

      // When
      ClaimCheckValue claimCheckValue =
          ClaimCheckValue.create(referenceUrl, originalSizeBytes);

      // Then
      assertThat(claimCheckValue).isNotNull();
      assertThat(claimCheckValue.getReferenceUrl()).isEqualTo(referenceUrl);
      assertThat(claimCheckValue.getOriginalSizeBytes()).isEqualTo(originalSizeBytes);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    @DisplayName("null 또는 빈 값의 ReferenceUrl을 인자로 넘기면 예외가 발생한다.")
    void nullOrBlankReferenceUrlCauseException(String referenceUrl) { // Given
      long originalSizeBytes = 1024 * 1024;

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> ClaimCheckValue.create(referenceUrl, originalSizeBytes))
          .withMessage("referenceUrl must be non-blank");
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, -2L, -3L})
    @DisplayName("originalSizeBytes를 0보다 낮게 설정하면 예외가 발생한다.")
    void setOriginalSizeBytesZeroCauseException(long originalSizeBytes) { // Given
      String referenceUrl = "s3://test-bucket/claim-checks";

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> ClaimCheckValue.create(referenceUrl, originalSizeBytes))
          .withMessage("originalSizeBytes must be >= 0");
    }
  }
}
