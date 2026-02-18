package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
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
          ClaimCheckSinkTransformTestConfigProvider.builder()
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
          ClaimCheckSinkTransformTestConfigProvider.builder()
              .storageType(ClaimCheckStorageType.S3.type())
              .build();
      transform.configure(configs);
    }

    @Test
    void shouldReturnUnchangedRecordWhenClaimCheckHeaderIsMissing() {
      // Given
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", null, null, 0);

      // When
      SinkRecord resultRecord = transform.apply(record);

      // Then
      assertThat(resultRecord).isEqualTo(record);
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

      String placeholderPayload = "{\"id\":0,\"name\":\"\"}";
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", null, placeholderPayload, 0);
      record.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When
      SinkRecord originalRecord = transform.apply(record);

      // Then
      Map<String, Object> expectedValue = new HashMap<>();
      expectedValue.put("id", 1);
      expectedValue.put("name", "cokelee777");

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(originalRecord.key()).isEqualTo("key");
      assertThat(originalRecord.valueSchema()).isNull();
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Map.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY)).isNull();
    }

    @Test
    void shouldRestoreOriginalRecordFromGenericSchemaClaimCheck() {
      // Given
      String originalPayload = "{\"id\":1,\"name\":\"cokelee777\"}";
      when(storage.retrieve(any())).thenReturn(originalPayload.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      SchemaAndValue claimCheckHeader =
          ClaimCheckHeader.toHeader(
              ClaimCheckMetadata.create(referenceUrl, originalPayload.length()));

      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      String placeholderPayload = "{\"id\":0,\"name\":\"\"}";
      SinkRecord record =
          new SinkRecord(
              "test-topic", 0, Schema.BYTES_SCHEMA, "key", valueSchema, placeholderPayload, 0);
      record.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When
      SinkRecord originalRecord = transform.apply(record);

      // Then
      Struct expectedValue = new Struct(valueSchema).put("id", 1L).put("name", "cokelee777");

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(originalRecord.key()).isEqualTo("key");
      assertThat(originalRecord.valueSchema()).isEqualTo(valueSchema);
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Struct.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY)).isNull();
    }

    @Test
    void shouldRestoreOriginalRecordFromDebeziumSchemaClaimCheck() {
      // Given
      String originalPayload =
          "{\"before\":{\"id\":1,\"name\":\"before cokelee777\"},\"after\":{\"id\":1,\"name\":\"after cokelee777\"},\"op\":\"c\",\"ts_ms\":1672531200000}";
      when(storage.retrieve(any())).thenReturn(originalPayload.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      SchemaAndValue claimCheckHeader =
          ClaimCheckHeader.toHeader(
              ClaimCheckMetadata.create(referenceUrl, originalPayload.length()));

      Schema rowSchema =
          SchemaBuilder.struct()
              .name("test.db.table.Value")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .optional()
              .build();
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("io.debezium.connector.mysql.Envelope")
              .field("before", rowSchema)
              .field("after", rowSchema)
              .field("op", Schema.STRING_SCHEMA)
              .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
              .build();
      String placeholderPayload =
          "{\"before\":{\"id\":0,\"name\":\"\"},\"after\":{\"id\":0,\"name\":\"\"},\"op\":\"\",\"ts_ms\":0}";
      SinkRecord record =
          new SinkRecord(
              "test-topic", 0, Schema.BYTES_SCHEMA, "key", valueSchema, placeholderPayload, 0);
      record.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When
      SinkRecord originalRecord = transform.apply(record);

      // Then
      Struct before = new Struct(rowSchema).put("id", 1L).put("name", "before cokelee777");
      Struct after = new Struct(rowSchema).put("id", 1L).put("name", "after cokelee777");
      Struct expectedValue =
          new Struct(valueSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", 1672531200000L);

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(originalRecord.key()).isEqualTo("key");
      assertThat(originalRecord.valueSchema()).isEqualTo(valueSchema);
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Struct.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY)).isNull();
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

      String placeholderPayload = "{\"id\":0,\"name\":\"\"}";
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", null, placeholderPayload, 0);
      record.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When & Then
      assertThatThrownBy(() -> transform.apply(record)).isInstanceOf(DataException.class);
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

      String placeholderPayload = "{\"id\":0,\"name\":\"\"}";
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", null, placeholderPayload, 0);
      record.headers().add(ClaimCheckHeader.HEADER_KEY, claimCheckHeader);

      // When & Then
      assertThatThrownBy(() -> transform.apply(record)).isInstanceOf(DataException.class);
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
