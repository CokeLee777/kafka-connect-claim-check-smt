package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config.ClaimCheckSinkTransformConfigFixture;
import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.record.RecordFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClaimCheckSinkTransformTest {

  @InjectMocks private ClaimCheckSinkTransform transform;
  @Mock private S3Storage storage;

  @Nested
  class ConfigureTest {

    @Test
    void shouldConfigureWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          ClaimCheckSinkTransformConfigFixture.builder()
              .storageType(ClaimCheckStorageType.S3.type())
              .build();

      // When
      transform.configure(configs);

      // Then
      assertThat(transform.getConfig().getStorageType()).isEqualTo(ClaimCheckStorageType.S3.type());
      assertThat(transform.getStorage()).isNotNull();
    }
  }

  @Nested
  class ApplyTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs =
          ClaimCheckSinkTransformConfigFixture.builder()
              .storageType(ClaimCheckStorageType.S3.type())
              .build();
      transform.configure(configs);
    }

    @Test
    void shouldReturnUnchangedRecordWhenClaimCheckHeaderIsMissing() {
      // Given
      SinkRecord initialSinkRecord =
          RecordFactory.sinkRecord(
              "test-topic",
              Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
              Map.of("id", 1L, "name", "cokelee777"));

      // When
      SinkRecord transformedSinkRecord = transform.apply(initialSinkRecord);

      // Then
      assertThat(transformedSinkRecord).isEqualTo(initialSinkRecord);
      verify(storage, never()).retrieve(any());
    }

    @Test
    void shouldRestoreOriginalRecordFromSchemalessClaimCheck() {
      // Given
      String originalPayload = "{\"id\":1,\"name\":\"cokelee777\"}";
      when(storage.retrieve(any())).thenReturn(originalPayload.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      SchemaAndValue claimCheckHeader =
          ClaimCheckHeader.toHeader(
              ClaimCheckMetadata.create(referenceUrl, originalPayload.length()));

      SinkRecord initialSinkRecord =
          RecordFactory.schemalessSinkRecord("test-topic", Map.of("id", 0L, "name", ""));
      initialSinkRecord.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When
      SinkRecord originalRecord = transform.apply(initialSinkRecord);

      // Then
      Map<String, Object> expectedValue = new HashMap<>();
      expectedValue.put("id", 1);
      expectedValue.put("name", "cokelee777");

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.valueSchema()).isNull();
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Map.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY)).isNull();
    }

    @Test
    void shouldRestoreOriginalRecordFromSchemaClaimCheck() {
      // Given
      String originalPayload = "{\"id\":1,\"name\":\"cokelee777\"}";
      when(storage.retrieve(any())).thenReturn(originalPayload.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      SchemaAndValue claimCheckHeader =
          ClaimCheckHeader.toHeader(
              ClaimCheckMetadata.create(referenceUrl, originalPayload.length()));

      SinkRecord initialSinkRecord =
          RecordFactory.sinkRecord(
              "test-topic",
              Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
              Map.of("id", 0L, "name", ""));
      initialSinkRecord.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When
      SinkRecord transformedSinkRecord = transform.apply(initialSinkRecord);

      // Then
      assertThat(transformedSinkRecord).isNotNull();
      assertThat(transformedSinkRecord.topic()).isEqualTo("test-topic");
      assertThat(transformedSinkRecord.valueSchema()).isEqualTo(initialSinkRecord.valueSchema());
      assertThat(transformedSinkRecord.value()).isNotNull();
      assertThat(transformedSinkRecord.value()).isInstanceOf(Struct.class);
      assertThat(((Struct) transformedSinkRecord.value()).get("id")).isEqualTo(1L);
      assertThat(((Struct) transformedSinkRecord.value()).get("name")).isEqualTo("cokelee777");
      assertThat(transformedSinkRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY))
          .isNull();
    }

    @Test
    void shouldThrowDataExceptionWhenRetrievedBytesIsNull() {
      // Given
      when(storage.retrieve(any())).thenReturn(null);

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      String originalPayload = "{\"id\":1,\"name\":\"cokelee777\"}";
      SchemaAndValue claimCheckHeader =
          ClaimCheckHeader.toHeader(
              ClaimCheckMetadata.create(referenceUrl, originalPayload.length()));

      SinkRecord initialSinkRecord =
          RecordFactory.sinkRecord(
              "test-topic",
              Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
              Map.of("id", 0L, "name", ""));
      initialSinkRecord.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When & Then
      assertThatThrownBy(() -> transform.apply(initialSinkRecord))
          .isInstanceOf(DataException.class);
    }

    @Test
    void shouldThrowDataExceptionWhenRetrievedBytesSizeMismatch() {
      // Given
      String originalPayload = "{\"id\":1,\"name\":\"cokelee777\"}";
      byte[] differentPayload =
          "{\"id\":1,\"name\":\"cokelee777-different\"}".getBytes(StandardCharsets.UTF_8);
      when(storage.retrieve(any())).thenReturn(differentPayload);

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      SchemaAndValue claimCheckHeader =
          ClaimCheckHeader.toHeader(
              ClaimCheckMetadata.create(referenceUrl, originalPayload.length()));

      SinkRecord initialSinkRecord =
          RecordFactory.sinkRecord(
              "test-topic",
              Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
              Map.of("id", 0L, "name", ""));
      initialSinkRecord.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When & Then
      assertThatThrownBy(() -> transform.apply(initialSinkRecord))
          .isInstanceOf(DataException.class);
    }
  }

  @Nested
  class CloseTest {

    @Test
    void shouldCloseStorageWhenInjected() {
      // Given & When
      transform.close();

      // Then
      verify(storage, times(1)).close();
    }

    @Test
    void shouldNotThrowExceptionWhenStorageIsNull() {
      // Given
      transform = new ClaimCheckSinkTransform();

      // When & Then
      assertDoesNotThrow(() -> transform.close());
    }
  }
}
