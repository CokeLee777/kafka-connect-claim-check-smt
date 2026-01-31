package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;

@DisplayName("S3ClientFactory 단위 테스트")
class S3ClientFactoryTest {

  private final S3ClientFactory s3ClientFactory = new S3ClientFactory();

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 S3Client가 생성된다.")
    public void rightConfig() {
      // Given
      Map<String, ? extends Serializable> originals =
          Map.of(
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.RETRY_MAX,
              3,
              S3Storage.Config.RETRY_BACKOFF_MS,
              300L,
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              20000L);
      SimpleConfig config = new SimpleConfig(S3Storage.Config.DEFINITION, originals);
      S3ClientConfig s3ClientConfig = S3Storage.Config.toS3ClientConfig(config);

      // When
      try (S3Client s3Client = s3ClientFactory.create(s3ClientConfig)) {
        // Then
        assertThat(s3Client).isNotNull();
      }
    }

    @Test
    @DisplayName("endpointOverride가 설정되면 forcePathStyle이 활성화된다.")
    public void withEndpointOverride() {
      // Given
      Map<String, ? extends Serializable> originals =
          Map.of(
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.ENDPOINT_OVERRIDE,
              "http://localhost:4566",
              S3Storage.Config.RETRY_MAX,
              3,
              S3Storage.Config.RETRY_BACKOFF_MS,
              300L,
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              20000L);
      SimpleConfig config = new SimpleConfig(S3Storage.Config.DEFINITION, originals);
      S3ClientConfig s3ClientConfig = S3Storage.Config.toS3ClientConfig(config);

      // When
      try (S3Client s3Client = s3ClientFactory.create(s3ClientConfig)) {
        // Then
        assertThat(s3Client).isNotNull();
      }
    }
  }

  @Nested
  @DisplayName("createOverrideConfiguration 메서드 테스트")
  class CreateOverrideConfigurationTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 ClientOverrideConfiguration이 생성된다.")
    public void normalConfig() {
      // Given
      Map<String, ? extends Serializable> originals =
          Map.of(
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.RETRY_MAX,
              3,
              S3Storage.Config.RETRY_BACKOFF_MS,
              300L,
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              20000L);
      SimpleConfig config = new SimpleConfig(S3Storage.Config.DEFINITION, originals);
      S3ClientConfig s3ClientConfig = S3Storage.Config.toS3ClientConfig(config);

      // When
      ClientOverrideConfiguration clientOverrideConfiguration =
          s3ClientFactory.createOverrideConfiguration(s3ClientConfig);

      // Then
      assertThat(clientOverrideConfiguration).isNotNull();
      assertThat(clientOverrideConfiguration.retryStrategy().isPresent()).isTrue();
    }
  }

  @Nested
  @DisplayName("createRetryStrategy 메서드 테스트")
  class CreateRetryStrategyTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 RetryStrategy가 생성된다.")
    public void normalConfig() {
      // Given
      final int INITIAL_ATTEMPT = 1;
      int maxAttempts = 3 + INITIAL_ATTEMPT;
      RetryConfig config =
          new RetryConfig(maxAttempts, Duration.ofMillis(300L), Duration.ofMillis(20000L));

      // When
      StandardRetryStrategy retryStrategy = s3ClientFactory.createRetryStrategy(config);

      // Then
      assertThat(retryStrategy).isNotNull();
      assertThat(retryStrategy.maxAttempts()).isEqualTo(maxAttempts);
    }
  }
}
