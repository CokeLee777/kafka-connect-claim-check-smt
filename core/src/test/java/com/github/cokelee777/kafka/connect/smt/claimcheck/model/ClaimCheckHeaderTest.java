package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ClaimCheckHeaderTest {

  @Nested
  class ToHeaderTest {

    @Test
    void shouldConvertToStringSchemaHeader() {
      // Given
      ClaimCheckMetadata metadata =
          new ClaimCheckMetadata("s3://test-bucket/claim-checks", 1024, 1700000000000L);

      // When
      SchemaAndValue header = ClaimCheckHeader.toHeader(metadata);

      // Then
      assertThat(header.schema()).isEqualTo(Schema.STRING_SCHEMA);
      assertThat(header.value()).isInstanceOf(String.class);
    }

    @Test
    void shouldContainAllFieldsInHeaderValue() {
      // Given
      ClaimCheckMetadata metadata =
          new ClaimCheckMetadata("s3://test-bucket/claim-checks", 1024, 1700000000000L);

      // When
      SchemaAndValue header = ClaimCheckHeader.toHeader(metadata);

      // Then
      assertThat((String) header.value())
          .contains("\"reference_url\":\"s3://test-bucket/claim-checks\"")
          .contains("\"original_size_bytes\":1024")
          .contains("\"uploaded_at\":1700000000000");
    }
  }

  @Nested
  class FromHeaderTest {

    @Test
    void shouldParseClaimCheckMetadataFromHeader() {
      // Given
      String json =
          "{\"reference_url\":\"s3://test-bucket/claim-checks\","
              + "\"original_size_bytes\":1024,"
              + "\"uploaded_at\":1700000000000}";
      Header header = mock(Header.class);
      when(header.value()).thenReturn(json);

      // When
      ClaimCheckMetadata metadata = ClaimCheckHeader.fromHeader(header);

      // Then
      assertThat(metadata.referenceUrl()).isEqualTo("s3://test-bucket/claim-checks");
      assertThat(metadata.originalSizeBytes()).isEqualTo(1024);
      assertThat(metadata.uploadedAt()).isEqualTo(1700000000000L);
    }

    @Test
    void shouldThrowExceptionWhenHeaderIsNull() {
      // Given
      Header header = null;

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckHeader.fromHeader(header))
          .withMessage("Header or header value is null");
    }

    @Test
    void shouldThrowExceptionWhenHeaderValueIsNull() {
      // Given
      Header header = mock(Header.class);
      when(header.value()).thenReturn(null);

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckHeader.fromHeader(header))
          .withMessage("Header or header value is null");
    }

    @Test
    void shouldThrowExceptionWhenHeaderValueIsNotString() {
      // Given
      Header header = mock(Header.class);
      when(header.value()).thenReturn(12345);

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> ClaimCheckHeader.fromHeader(header))
          .withMessage("Expected String header value, got: Integer");
    }
  }
}
