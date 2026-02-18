package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClaimCheckSourceTransformTest {

  @InjectMocks private ClaimCheckSourceTransform transform;
  @Mock private S3Storage storage;

  @Nested
  class ConfigureTest {

    @Test
    void shouldConfigureWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          ClaimCheckSourceTransformTestConfigProvider.builder()
              .storageType(ClaimCheckStorageType.S3.type())
              .thresholdBytes(1024)
              .build();

      // When
      transform.configure(configs);

      // Then
      assertThat(transform.getConfig().getStorageType()).isEqualTo(ClaimCheckStorageType.S3.type());
      assertThat(transform.getConfig().getThresholdBytes()).isEqualTo(1024);
      assertThat(transform.getStorage()).isNotNull();
    }
  }

  @Nested
  class ApplyTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs =
          ClaimCheckSourceTransformTestConfigProvider.builder()
              .storageType(ClaimCheckStorageType.S3.type())
              .thresholdBytes(1)
              .build();
      transform.configure(configs);
    }

    @Test
    void shouldCreateClaimCheckRecordFromSchemalessSourceRecord() {
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
      Header claimCheckHeader =
          claimCheckRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
    }

    @Test
    void shouldCreateClaimCheckRecordFromGenericSchemaSourceRecord() {
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
      assertThat(claimCheckRecord.valueSchema()).isEqualTo(valueSchema);
      assertThat(claimCheckRecord.value()).isNotNull();
      assertThat(claimCheckRecord.value()).isInstanceOf(Struct.class);
      Header claimCheckHeader =
          claimCheckRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
    }

    @Test
    void shouldCreateClaimCheckRecordFromDebeziumSchemaSourceRecord() {
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
      Schema valueSchema =
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
          new Struct(valueSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", tsMs);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, envelope);

      // When
      SourceRecord claimCheckRecord = transform.apply(record);

      // Then
      assertThat(claimCheckRecord).isNotNull();
      assertThat(claimCheckRecord.topic()).isEqualTo("test-topic");
      assertThat(claimCheckRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(claimCheckRecord.key()).isEqualTo("key");
      assertThat(claimCheckRecord.valueSchema()).isEqualTo(valueSchema);
      assertThat(claimCheckRecord.value()).isNotNull();
      assertThat(claimCheckRecord.value()).isInstanceOf(Struct.class);
      Header claimCheckHeader =
          claimCheckRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
    }

    @Test
    void shouldPreserveExistingHeadersAndAddClaimCheckHeader() {
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

      // Add a custom header
      record.headers().add("custom-header", "custom-value", Schema.STRING_SCHEMA);

      // When
      SourceRecord claimCheckRecord = transform.apply(record);

      // Then
      assertThat(claimCheckRecord).isNotNull();
      assertThat(claimCheckRecord.headers().size()).isEqualTo(2);
      Header customHeader = claimCheckRecord.headers().lastWithName("custom-header");
      assertThat(customHeader).isNotNull();
      assertThat(customHeader.key()).isEqualTo("custom-header");
      assertThat(customHeader.value()).isEqualTo("custom-value");
      assertThat(customHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);

      Header claimCheckHeader =
          claimCheckRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
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
      transform = new ClaimCheckSourceTransform();

      // When & Then
      assertDoesNotThrow(() -> transform.close());
    }
  }
}
