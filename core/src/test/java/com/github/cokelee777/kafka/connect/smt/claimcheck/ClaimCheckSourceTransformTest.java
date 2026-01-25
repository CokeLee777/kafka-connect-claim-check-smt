package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("ClaimCheckSourceTransform 단위 테스트")
class ClaimCheckSourceTransformTest {

  @InjectMocks private ClaimCheckSourceTransform transform;
  @Mock private ClaimCheckStorage storage;

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTest {

    @Test
    @DisplayName("올바른 설정정보를 구성하면 정상적으로 구성된다.")
    void rightConfig() {
      // Given
      Map<String, String> configs =
          Map.of(
              ClaimCheckSourceTransform.Config.STORAGE_TYPE,
              ClaimCheckStorageType.S3.type(),
              ClaimCheckSourceTransform.Config.THRESHOLD_BYTES,
              "1024",
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.PATH_PREFIX,
              "test/path",
              S3Storage.Config.RETRY_MAX,
              "3",
              S3Storage.Config.RETRY_BACKOFF_MS,
              "300",
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              "20000");

      // When
      transform.configure(configs);

      // Then
      assertThat(transform.getStorageType()).isEqualTo(ClaimCheckStorageType.S3.type());
      assertThat(transform.getThresholdBytes()).isEqualTo(1024);
      assertThat(transform.getStorage()).isNotNull();
      assertThat(transform.getRecordSerializer()).isNotNull();
    }
  }

  @Nested
  @DisplayName("apply 메서드 테스트")
  class ApplyTest {

    @BeforeEach
    void beforeEach() {
      Map<String, String> configs =
          Map.of(
              ClaimCheckSourceTransform.Config.STORAGE_TYPE,
              ClaimCheckStorageType.S3.type(),
              ClaimCheckSourceTransform.Config.THRESHOLD_BYTES,
              "1",
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.PATH_PREFIX,
              "test/path",
              S3Storage.Config.RETRY_MAX,
              "3",
              S3Storage.Config.RETRY_BACKOFF_MS,
              "300",
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              "20000");
      transform.configure(configs);
    }

    @Test
    @DisplayName("Schemaless SourceRecord를 인자로 넣으면 ClaimCheckRecord가 생성된다.")
    void schemalessSourceRecordReturnClaimCheckRecord() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);

      Map<String, Object> value = new HashMap<>();
      value.put("id", 1L);
      value.put("name", "cokelee777");
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      SourceRecord claimCheckRecord = transform.apply(record);

      // Then
      assertThat(claimCheckRecord).isNotNull();
      assertThat(claimCheckRecord.topic()).isEqualTo("test-topic");
      assertThat(claimCheckRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(claimCheckRecord.key()).isEqualTo("key");
      assertThat(claimCheckRecord.valueSchema()).isNull();
      assertThat(claimCheckRecord.value()).isNull();
      Header claimCheckHeader = claimCheckRecord.headers().lastWithName(ClaimCheckSchema.NAME);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
      assertThat(claimCheckHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    }

    @Test
    @DisplayName("flatSchema SourceRecord를 인자로 넣으면 ClaimCheckRecord가 생성된다.")
    void flatSchemaSourceRecordReturnClaimCheckRecord() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);

      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      Struct value = new Struct(valueSchema).put("id", 1L).put("name", "cokelee777");
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When
      SourceRecord claimCheckRecord = transform.apply(record);

      // Then
      assertThat(claimCheckRecord).isNotNull();
      assertThat(claimCheckRecord.topic()).isEqualTo("test-topic");
      assertThat(claimCheckRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(claimCheckRecord.key()).isEqualTo("key");
      assertThat(claimCheckRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
      assertThat(claimCheckRecord.value()).isInstanceOf(Struct.class);
      assertThat(((Struct) claimCheckRecord.value()).get("id")).isEqualTo(0L);
      assertThat(((Struct) claimCheckRecord.value()).get("name")).isEqualTo("");
      Header claimCheckHeader = claimCheckRecord.headers().lastWithName(ClaimCheckSchema.NAME);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
      assertThat(claimCheckHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    }

    @Test
    @DisplayName("DebeziumSchema SourceRecord를 인자로 넣으면 ClaimCheckRecord가 생성된다.")
    void debeziumSchemaSourceRecordReturnClaimCheckRecord() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);

      Schema rowSchema =
          SchemaBuilder.struct()
              .name("test.db.table.Value")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .optional()
              .build();
      Schema envelopeSchema =
          SchemaBuilder.struct()
              .name("io.debezium.connector.mysql.Envelope")
              .field("before", rowSchema)
              .field("after", rowSchema)
              .field("op", Schema.STRING_SCHEMA)
              .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
              .build();
      Struct before = new Struct(rowSchema).put("id", 1L).put("name", "before cokelee777");
      Struct after = new Struct(rowSchema).put("id", 1L).put("name", "after cokelee777");
      long tsMs = System.currentTimeMillis();
      Struct envelope =
          new Struct(envelopeSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", tsMs);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", envelopeSchema, envelope);

      // When
      SourceRecord claimCheckRecord = transform.apply(record);

      // Then
      assertThat(claimCheckRecord).isNotNull();
      assertThat(claimCheckRecord.topic()).isEqualTo("test-topic");
      assertThat(claimCheckRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(claimCheckRecord.key()).isEqualTo("key");
      assertThat(claimCheckRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
      assertThat(claimCheckRecord.value()).isInstanceOf(Struct.class);
      assertThat(((Struct) claimCheckRecord.value()).getStruct("before")).isNull();
      assertThat(((Struct) claimCheckRecord.value()).getStruct("after")).isNull();
      assertThat(((Struct) claimCheckRecord.value()).getString("op")).isEqualTo("c");
      assertThat(((Struct) claimCheckRecord.value()).getInt64("ts_ms")).isEqualTo(tsMs);
      Header claimCheckHeader = claimCheckRecord.headers().lastWithName(ClaimCheckSchema.NAME);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
      assertThat(claimCheckHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    }

    @Test
    @DisplayName("지원하지 않는 Schema SourceRecord를 인자로 넣으면 예외가 발생한다.")
    void unsupportedSchemaSourceRecordReturnClaimCheckRecord() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);

      Schema valueSchema = Schema.STRING_SCHEMA;
      String value = "{\"id\":1,\"name\":\"cokelee777\"}";
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When & Then
      assertThatExceptionOfType(IllegalStateException.class)
          .isThrownBy(() -> transform.apply(record))
          .withMessage(
              "No strategy found for schema: "
                  + valueSchema
                  + ", schema type: "
                  + valueSchema.type());
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTest {

    @Test
    @DisplayName("ClaimCheckStorage가 주입된 상태에서 close 호출 시 storage의 close가 호출된다.")
    void shouldCloseInjectedClaimCheckStorage() {
      // Given & When
      transform.close();

      // Then
      verify(storage, times(1)).close();
    }

    @Test
    @DisplayName("ClaimCheckStorage가 null이어도 예외가 발생하지 않는다.")
    void notCauseExceptionAndCloseWhenClaimCheckStorageIsNull() {
      // Given
      transform = new ClaimCheckSourceTransform();

      // When & Then
      assertDoesNotThrow(() -> transform.close());
    }
  }
}
