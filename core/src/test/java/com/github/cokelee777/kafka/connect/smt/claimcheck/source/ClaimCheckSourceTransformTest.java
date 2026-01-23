package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchemaFields;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
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
              StorageType.S3.type(),
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
      assertThat(transform.getStorageType()).isEqualTo(StorageType.S3.type());
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
              StorageType.S3.type(),
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
    @DisplayName("올바른 SourceRecord를 인자로 넣으면 ClaimCheckRecord가 생성된다.")
    void rightSourceRecordReturnClaimCheckRecord() {
      // Given
      String referenceUrl = "s3://test-bucket/test/path/uuid";
      when(storage.store(any())).thenReturn(referenceUrl);
      SourceRecord record =
          new SourceRecord(
              null,
              null,
              "test-bucket",
              Schema.BYTES_SCHEMA,
              "key",
              Schema.STRING_SCHEMA,
              "payload");

      // When
      SourceRecord claimCheckRecord = transform.apply(record);

      // Then
      assertThat(claimCheckRecord).isNotNull();
      assertThat(claimCheckRecord.topic()).isEqualTo("test-bucket");
      assertThat(claimCheckRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(claimCheckRecord.key()).isEqualTo("key");
      assertThat(claimCheckRecord.valueSchema()).isNotEqualTo(Schema.STRING_SCHEMA);
      assertThat(claimCheckRecord.valueSchema()).isEqualTo(ClaimCheckSchema.SCHEMA);
      assertThat(claimCheckRecord.value()).isNotEqualTo("payload");
      assertThat(claimCheckRecord.value()).isInstanceOf(Struct.class);
      assertThat(((Struct) claimCheckRecord.value()).get(ClaimCheckSchemaFields.REFERENCE_URL))
          .isEqualTo(referenceUrl);
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
