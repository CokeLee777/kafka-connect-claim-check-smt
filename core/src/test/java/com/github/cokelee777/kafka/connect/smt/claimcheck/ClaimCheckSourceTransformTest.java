package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config.ClaimCheckSourceTransformConfigFixture;
import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.record.RecordFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
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
          ClaimCheckSourceTransformConfigFixture.builder()
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
          ClaimCheckSourceTransformConfigFixture.builder()
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

      SourceRecord initialSourceRecord =
          RecordFactory.schemalessSourceRecord(
              "test-topic", Map.of("id", 1L, "name", "cokelee777"));

      // When
      SourceRecord transformedRecord = transform.apply(initialSourceRecord);

      // Then
      assertThat(transformedRecord).isNotNull();
      assertThat(transformedRecord.topic()).isEqualTo("test-topic");
      assertThat(transformedRecord.valueSchema()).isNull();
      assertThat(transformedRecord.value()).isNull();
      Header claimCheckHeader =
          transformedRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
    }

    @Test
    void shouldCreateClaimCheckRecordFromSchemaSourceRecord() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);

      SourceRecord initialSourceRecord =
          RecordFactory.sourceRecord(
              "test-topic",
              Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
              Map.of("id", 1L, "name", "cokelee777"));

      // When
      SourceRecord transformedRecord = transform.apply(initialSourceRecord);

      // Then
      assertThat(transformedRecord).isNotNull();
      assertThat(transformedRecord.topic()).isEqualTo("test-topic");
      assertThat(transformedRecord.valueSchema()).isNotNull();
      assertThat(transformedRecord.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
      assertThat(transformedRecord.value()).isNotNull();
      assertThat(transformedRecord.value()).isInstanceOf(Struct.class);
      Header claimCheckHeader =
          transformedRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader).isNotNull();
      assertThat(claimCheckHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
      assertThat(claimCheckHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
    }

    @Test
    void shouldPreserveExistingHeadersAndAddClaimCheckHeader() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);

      SourceRecord initialSourceRecord =
          RecordFactory.sourceRecord(
              "test-topic",
              Map.of("id", Schema.INT64_SCHEMA, "name", Schema.STRING_SCHEMA),
              Map.of("id", 1L, "name", "cokelee777"));
      initialSourceRecord.headers().add("custom-header", "custom-value", Schema.STRING_SCHEMA);

      // When
      SourceRecord transformedSourceRecord = transform.apply(initialSourceRecord);

      // Then
      assertThat(transformedSourceRecord).isNotNull();
      assertThat(transformedSourceRecord.headers().size()).isEqualTo(2);
      Header customHeader = transformedSourceRecord.headers().lastWithName("custom-header");
      assertThat(customHeader).isNotNull();
      assertThat(customHeader.key()).isEqualTo("custom-header");
      assertThat(customHeader.value()).isEqualTo("custom-value");
      assertThat(customHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);

      Header claimCheckHeader =
          transformedSourceRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
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
