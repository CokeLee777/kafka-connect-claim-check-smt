package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.S3StorageTestConfigProvider;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors.ClaimCheckStoreException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class S3StorageTest {

  @InjectMocks private S3Storage s3Storage;
  @Mock private S3Client s3Client;

  @Nested
  class ConfigureTest {

    @Test
    void shouldConfigureWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          S3StorageTestConfigProvider.builder()
              .bucketName("test-bucket")
              .region(Region.AP_NORTHEAST_1.id())
              .pathPrefix("/test/path")
              .retryMax(5)
              .retryBackoffMs(500L)
              .retryMaxBackoffMs(30000L)
              .build();

      // When
      s3Storage.configure(configs);

      // Then
      assertThat(s3Storage.getConfig().getBucketName()).isEqualTo("test-bucket");
      assertThat(s3Storage.getConfig().getRegion()).isEqualTo(Region.AP_NORTHEAST_1.id());
      assertThat(s3Storage.getConfig().getPathPrefix()).isEqualTo("test/path");
      assertThat(s3Storage.getConfig().getRetryMax()).isEqualTo(5);
      assertThat(s3Storage.getConfig().getRetryBackoffMs()).isEqualTo(500L);
      assertThat(s3Storage.getConfig().getRetryMaxBackoffMs()).isEqualTo(30000L);
    }

    @Test
    void shouldUseDefaultValuesWhenOnlyBucketNameProvided() {
      // Given
      Map<String, String> configs =
          S3StorageTestConfigProvider.builder().bucketName("test-bucket").build();

      // When
      s3Storage.configure(configs);

      // Then
      assertThat(s3Storage.getConfig().getBucketName()).isEqualTo("test-bucket");
      assertThat(s3Storage.getConfig().getRegion()).isEqualTo(S3StorageConfig.REGION_DEFAULT);
      assertThat(s3Storage.getConfig().getPathPrefix())
          .isEqualTo(S3StorageConfig.PATH_PREFIX_DEFAULT);
      assertThat(s3Storage.getConfig().getRetryMax()).isEqualTo(S3StorageConfig.RETRY_MAX_DEFAULT);
      assertThat(s3Storage.getConfig().getRetryBackoffMs())
          .isEqualTo(S3StorageConfig.RETRY_BACKOFF_MS_DEFAULT);
      assertThat(s3Storage.getConfig().getRetryMaxBackoffMs())
          .isEqualTo(S3StorageConfig.RETRY_MAX_BACKOFF_MS_DEFAULT);
    }

    @Test
    void shouldThrowExceptionWhenBucketNameIsMissing() {
      // Given
      Map<String, String> configs = S3StorageTestConfigProvider.builder().build();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> s3Storage.configure(configs))
          .withMessage(
              "Missing required configuration \"storage.s3.bucket.name\" which has no default value.");
    }
  }

  @Nested
  class StoreTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs =
          S3StorageTestConfigProvider.builder().bucketName("test-bucket").build();
      s3Storage.configure(configs);
    }

    @Test
    void shouldStorePayloadAndReturnS3ReferenceUrl() {
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
    void shouldThrowRuntimeExceptionWhenS3UploadFails() {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenThrow(S3Exception.builder().message("Test S3 Exception").build());

      // When & Then
      assertThatExceptionOfType(ClaimCheckStoreException.class)
          .isThrownBy(() -> s3Storage.store(payload))
          .withMessageStartingWith("Failed to upload to S3. Bucket: test-bucket");
    }
  }

  @Nested
  class RetrieveTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs =
          S3StorageTestConfigProvider.builder().bucketName("test-bucket").build();
      s3Storage.configure(configs);
    }

    @Test
    void shouldRetrieveStoredPayload() throws IOException {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      String key = "test-key";
      String referenceUrl = "s3://test-bucket/" + key;

      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket("test-bucket").key(key).build();
      ResponseInputStream<GetObjectResponse> responseStream = mock(ResponseInputStream.class);
      when(s3Client.getObject(getObjectRequest)).thenReturn(responseStream);
      when(responseStream.readAllBytes()).thenReturn(payload);

      // When
      byte[] retrievedPayload = s3Storage.retrieve(referenceUrl);

      // Then
      assertThat(retrievedPayload).isEqualTo(payload);
      verify(s3Client, times(1)).getObject(getObjectRequest);
    }

    @Test
    void shouldThrowDataExceptionWhenUrlSchemeIsInvalid() {
      // Given
      String invalidUrl = "file://test-bucket/test-key";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> s3Storage.retrieve(invalidUrl))
          .withMessage("S3 reference URL must start with 's3://'");
    }

    @Test
    void shouldThrowDataExceptionWhenUrlFormatIsInvalid() {
      // Given
      String invalidUrl1 = "s3://test-bucket"; // No key
      String invalidUrl2 = "s3:///test-key"; // No bucket
      String invalidUrl3 = "s3://test-bucket/"; // Trailing slash

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> s3Storage.retrieve(invalidUrl1))
          .withMessage("Invalid S3 reference URL: " + invalidUrl1);
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> s3Storage.retrieve(invalidUrl2))
          .withMessage("Invalid S3 reference URL: " + invalidUrl2);
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> s3Storage.retrieve(invalidUrl3))
          .withMessage("Invalid S3 reference URL: " + invalidUrl3);
    }

    @Test
    void shouldThrowDataExceptionWhenBucketInUrlDoesNotMatchConfiguredBucket() {
      // Given
      String referenceUrl = "s3://other-bucket/test-key";

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> s3Storage.retrieve(referenceUrl))
          .withMessage(
              String.format(
                  "Bucket in reference URL ('%s') does not match configured bucket ('%s')",
                  "other-bucket", "test-bucket"));
    }
  }

  @Nested
  class CloseTest {

    @Test
    void shouldCloseS3Client() {
      // Given & When
      s3Storage.close();

      // Then
      verify(s3Client, times(1)).close();
    }

    @Test
    void shouldNotThrowExceptionWhenS3ClientIsNull() {
      // Given
      s3Storage = new S3Storage();

      // When & Then
      assertDoesNotThrow(() -> s3Storage.close());
    }
  }
}
