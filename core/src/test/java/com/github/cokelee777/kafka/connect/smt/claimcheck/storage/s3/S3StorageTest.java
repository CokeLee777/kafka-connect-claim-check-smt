package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3Storage 단위 테스트")
class S3StorageTest {

  @InjectMocks private S3Storage s3Storage;
  @Mock private S3Client s3Client;

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 구성된다.")
    void rightConfig() {
      // Given
      Map<String, String> configs =
          Map.of(
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
      s3Storage.configure(configs);

      // Then
      assertThat(s3Storage.getBucketName()).isEqualTo("test-bucket");
      assertThat(s3Storage.getPathPrefix()).isEqualTo("test/path");
    }

    @Test
    @DisplayName("설정정보에 버킷명만 존재해도 정상적으로 구성된다.")
    void onlyBucketNameConfig() {
      // Given
      Map<String, String> configs = Map.of(S3Storage.Config.BUCKET_NAME, "test-bucket");

      // When
      s3Storage.configure(configs);

      // Then
      assertThat(s3Storage.getBucketName()).isEqualTo("test-bucket");
      assertThat(s3Storage.getPathPrefix()).isEqualTo(S3Storage.Config.DEFAULT_PATH_PREFIX);
    }

    @Test
    @DisplayName("설정정보에 버킷명이 없으면 예외가 발생한다.")
    void withoutBucketNameCauseException() {
      // Given
      Map<String, String> configs =
          Map.of(
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

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> s3Storage.configure(configs))
          .withMessage(
              "Missing required configuration \"storage.s3.bucket.name\" which has no default value.");
    }
  }

  @Nested
  @DisplayName("store 메서드 테스트")
  class StoreTest {

    @BeforeEach
    void beforeEach() {
      Map<String, String> configs = Map.of(S3Storage.Config.BUCKET_NAME, "test-bucket");
      s3Storage.configure(configs);
    }

    @Test
    @DisplayName("올바른 payload를 인자로 넘기면 정상적으로 S3에 저장된다.")
    void rightPayloadCauseS3Store() {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      String referenceUrl = s3Storage.store(payload);

      // Then
      verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
      assertThat(referenceUrl).isNotBlank();
      assertThat(referenceUrl).startsWith("s3://");
    }

    @Test
    @DisplayName("S3에 payload를 업로드 중에 예외가 발생하면 RuntimeException이 발생한다.")
    void s3UploadExceptionCauseRuntimeException() {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenThrow(S3Exception.builder().message("Test S3 Exception").build());

      // When & Then
      assertThatExceptionOfType(RuntimeException.class)
          .isThrownBy(() -> s3Storage.store(payload))
          .withMessageStartingWith("Failed to upload to S3. Bucket: test-bucket");
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTest {

    @Test
    @DisplayName("S3Client가 주입된 상태에서 close 호출 시 client의 close가 호출된다.")
    void shouldCloseInjectedClient() {
      // Given & When
      s3Storage.close();

      // Then
      verify(s3Client, times(1)).close();
    }

    @Test
    @DisplayName("S3Client가 null이어도 예외가 발생하지 않는다.")
    void notCauseExceptionAndCloseWhenS3ClientIsNull() {
      // Given
      s3Storage = new S3Storage();

      // When & Then
      assertDoesNotThrow(() -> s3Storage.close());
    }
  }
}
