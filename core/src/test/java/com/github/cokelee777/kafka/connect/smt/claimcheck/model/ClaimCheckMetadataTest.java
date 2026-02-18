package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class ClaimCheckMetadataTest {

  @Nested
  class CreateTest {

    @Test
    void shouldCreateClaimCheckMetadata() {
      // Given
      String referenceUrl = "s3://test-bucket/claim-checks";
      int originalSizeBytes = 1024 * 1024;

      // When
      ClaimCheckMetadata claimCheckMetadata =
          ClaimCheckMetadata.create(referenceUrl, originalSizeBytes);

      // Then
      assertThat(claimCheckMetadata).isNotNull();
      assertThat(claimCheckMetadata.referenceUrl()).isEqualTo(referenceUrl);
      assertThat(claimCheckMetadata.originalSizeBytes()).isEqualTo(originalSizeBytes);
      assertThat(claimCheckMetadata.uploadedAt()).isPositive();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void shouldThrowExceptionWhenReferenceUrlIsBlank(String referenceUrl) { // Given
      int originalSizeBytes = 1024 * 1024;

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.create(referenceUrl, originalSizeBytes))
          .withMessage("referenceUrl must be non-blank");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -2, -3})
    void shouldThrowExceptionWhenOriginalSizeBytesIsNegative(int originalSizeBytes) { // Given
      String referenceUrl = "s3://test-bucket/claim-checks";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.create(referenceUrl, originalSizeBytes))
          .withMessage("originalSizeBytes must be >= 0");
    }
  }

  @Nested
  class ToJsonTest {

    @Test
    void shouldSerializeToJson() {
      // Given
      ClaimCheckMetadata claimCheckMetadata =
          new ClaimCheckMetadata("s3://test-bucket/claim-checks", 1024, 1700000000000L);

      // When
      String json = claimCheckMetadata.toJson();

      // Then
      assertThat(json)
          .contains("\"reference_url\":\"s3://test-bucket/claim-checks\"")
          .contains("\"original_size_bytes\":1024")
          .contains("\"uploaded_at\":1700000000000");
    }
  }

  @Nested
  class FromJsonTest {

    @Test
    void shouldDeserializeFromJson() {
      // Given
      String json =
          "{\"reference_url\":\"s3://test-bucket/claim-checks\","
              + "\"original_size_bytes\":1024,"
              + "\"uploaded_at\":1700000000000}";

      // When
      ClaimCheckMetadata claimCheckMetadata = ClaimCheckMetadata.fromJson(json);

      // Then
      assertThat(claimCheckMetadata.referenceUrl()).isEqualTo("s3://test-bucket/claim-checks");
      assertThat(claimCheckMetadata.originalSizeBytes()).isEqualTo(1024);
      assertThat(claimCheckMetadata.uploadedAt()).isEqualTo(1700000000000L);
    }

    @Test
    void shouldThrowExceptionWhenJsonIsMalformed() {
      // Given
      String json = "invalid-json";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.fromJson(json))
          .withMessage("Failed to parse claim check JSON");
    }

    @Test
    void shouldThrowExceptionWhenRequiredFieldIsMissing() {
      // Given
      String json = "{\"reference_url\":\"s3://test-bucket/claim-checks\"}";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.fromJson(json))
          .withMessage("Missing required fields in claim check JSON");
    }

    @Test
    void shouldThrowExceptionWhenReferenceUrlTypeIsInvalid() {
      // Given
      String json =
          "{\"reference_url\":123,"
              + "\"original_size_bytes\":1024,"
              + "\"uploaded_at\":1700000000000}";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.fromJson(json))
          .withMessageContaining("reference_url")
          .withMessageContaining("expected STRING");
    }

    @Test
    void shouldThrowExceptionWhenOriginalSizeBytesTypeIsInvalid() {
      // Given
      String json =
          "{\"reference_url\":\"s3://test-bucket/claim-checks\","
              + "\"original_size_bytes\":\"not-a-number\","
              + "\"uploaded_at\":1700000000000}";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.fromJson(json))
          .withMessageContaining("original_size_bytes")
          .withMessageContaining("expected INT");
    }

    @Test
    void shouldThrowExceptionWhenUploadedAtTypeIsInvalid() {
      // Given
      String json =
          "{\"reference_url\":\"s3://test-bucket/claim-checks\","
              + "\"original_size_bytes\":1024,"
              + "\"uploaded_at\":\"not-a-number\"}";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckMetadata.fromJson(json))
          .withMessageContaining("uploaded_at")
          .withMessageContaining("expected LONG");
    }
  }
}
