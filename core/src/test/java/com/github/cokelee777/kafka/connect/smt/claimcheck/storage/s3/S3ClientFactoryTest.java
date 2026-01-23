package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
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
      S3ClientConfig config = new S3ClientConfig("ap-northeast-2", null, 3, 300L, 20000L);

      // When
      try (S3Client s3Client = s3ClientFactory.create(config)) {
        // Then
        assertThat(s3Client).isNotNull();
      }
    }

    @Test
    @DisplayName("endpointOverride가 설정되면 forcePathStyle이 활성화된다.")
    public void withEndpointOverride() {
      // Given
      S3ClientConfig config =
          new S3ClientConfig("ap-northeast-2", "http://localhost:4566", 3, 300L, 20000L);

      // When
      try (S3Client s3Client = s3ClientFactory.create(config)) {
        // Then
        assertThat(s3Client).isNotNull();
      }
    }
  }

  @Nested
  @DisplayName("createOverrideConfiguration 메서드 테스트")
  class CreateOverrideConfiguration {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 ClientOverrideConfiguration이 생성된다.")
    public void normalConfig() {
      // Given
      S3ClientConfig config = new S3ClientConfig("ap-northeast-2", null, 3, 300L, 20000L);

      // When
      ClientOverrideConfiguration clientOverrideConfiguration =
          s3ClientFactory.createOverrideConfiguration(config);

      // Then
      assertThat(clientOverrideConfiguration).isNotNull();
    }
  }
}
