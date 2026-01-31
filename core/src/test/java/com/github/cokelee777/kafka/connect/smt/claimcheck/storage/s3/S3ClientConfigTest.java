package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.util.Map;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("S3ClientConfig 단위 테스트")
class S3ClientConfigTest {

  @Nested
  @DisplayName("from 메서드 테스트")
  class FromTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 S3ClientConfig가 생성된다.")
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

      // When
      S3ClientConfig s3ClientConfig = S3Storage.Config.toS3ClientConfig(config);

      // Then
      assertThat(s3ClientConfig).isNotNull();
      assertThat(s3ClientConfig.region()).isEqualTo("ap-northeast-2");
      assertThat(s3ClientConfig.endpointOverride()).isNull();
      assertThat(s3ClientConfig.retryMax()).isEqualTo(3);
      assertThat(s3ClientConfig.retryBackoffMs()).isEqualTo(300L);
      assertThat(s3ClientConfig.retryMaxBackoffMs()).isEqualTo(20000L);
    }

    @Test
    @DisplayName("endpointOverride가 설정되어도 정상적으로 S3ClientConfig가 생성된다.")
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

      // When
      S3ClientConfig s3ClientConfig = S3Storage.Config.toS3ClientConfig(config);

      // Then
      assertThat(s3ClientConfig).isNotNull();
      assertThat(s3ClientConfig.region()).isEqualTo("ap-northeast-2");
      assertThat(s3ClientConfig.endpointOverride()).isEqualTo("http://localhost:4566");
      assertThat(s3ClientConfig.retryMax()).isEqualTo(3);
      assertThat(s3ClientConfig.retryBackoffMs()).isEqualTo(300L);
      assertThat(s3ClientConfig.retryMaxBackoffMs()).isEqualTo(20000L);
    }
  }
}
