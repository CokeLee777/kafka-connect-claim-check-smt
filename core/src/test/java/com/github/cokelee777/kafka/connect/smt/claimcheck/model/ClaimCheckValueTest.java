package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class ClaimCheckValueTest {

  @Nested
  class CreateTest {

    @Test
    void shouldCreateClaimCheckValue() {
      // Given
      String referenceUrl = "s3://test-bucket/claim-checks";
      int originalSizeBytes = 1024 * 1024;

      // When
      ClaimCheckValue claimCheckValue = ClaimCheckValue.create(referenceUrl, originalSizeBytes);

      // Then
      assertThat(claimCheckValue).isNotNull();
      assertThat(claimCheckValue.referenceUrl()).isEqualTo(referenceUrl);
      assertThat(claimCheckValue.originalSizeBytes()).isEqualTo(originalSizeBytes);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void shouldThrowExceptionWhenReferenceUrlIsBlank(String referenceUrl) { // Given
      int originalSizeBytes = 1024 * 1024;

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckValue.create(referenceUrl, originalSizeBytes))
          .withMessage("referenceUrl must be non-blank");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -2, -3})
    void shouldThrowExceptionWhenOriginalSizeBytesIsNegative(int originalSizeBytes) { // Given
      String referenceUrl = "s3://test-bucket/claim-checks";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckValue.create(referenceUrl, originalSizeBytes))
          .withMessage("originalSizeBytes must be >= 0");
    }
  }
}
