package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3ClientConfigFactory 단위 테스트")
class S3ClientConfigFactoryTest {

  @Mock private SimpleConfig config;

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 S3ClientConfig가 생성된다.")
    public void rightConfig() {
      // Given
      when(config.getString(S3Storage.Config.REGION)).thenReturn("ap-northeast-2");
      when(config.getString(S3Storage.Config.ENDPOINT_OVERRIDE)).thenReturn(null);
      when(config.getInt(S3Storage.Config.RETRY_MAX)).thenReturn(3);
      when(config.getLong(S3Storage.Config.RETRY_BACKOFF_MS)).thenReturn(300L);
      when(config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS)).thenReturn(20000L);

      // When
      S3ClientConfig s3ClientConfig = S3ClientConfigFactory.create(config);

      // Then
      assertThat(s3ClientConfig).isNotNull();
      assertThat(s3ClientConfig.getRegion()).isEqualTo("ap-northeast-2");
      assertThat(s3ClientConfig.getEndpointOverride()).isNull();
      assertThat(s3ClientConfig.getRetryMax()).isEqualTo(3);
      assertThat(s3ClientConfig.getRetryBackoffMs()).isEqualTo(300L);
      assertThat(s3ClientConfig.getRetryMaxBackoffMs()).isEqualTo(20000L);
    }

    @Test
    @DisplayName("endpointOverride가 설정되어도 정상적으로 S3ClientConfig가 생성된다.")
    public void withEndpointOverride() {
      // Given
      when(config.getString(S3Storage.Config.REGION)).thenReturn("ap-northeast-2");
      when(config.getString(S3Storage.Config.ENDPOINT_OVERRIDE))
          .thenReturn("http://localhost:4566");
      when(config.getInt(S3Storage.Config.RETRY_MAX)).thenReturn(3);
      when(config.getLong(S3Storage.Config.RETRY_BACKOFF_MS)).thenReturn(300L);
      when(config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS)).thenReturn(20000L);

      // When
      S3ClientConfig s3ClientConfig = S3ClientConfigFactory.create(config);

      // Then
      assertThat(s3ClientConfig).isNotNull();
      assertThat(s3ClientConfig.getRegion()).isEqualTo("ap-northeast-2");
      assertThat(s3ClientConfig.getEndpointOverride()).isEqualTo("http://localhost:4566");
      assertThat(s3ClientConfig.getRetryMax()).isEqualTo(3);
      assertThat(s3ClientConfig.getRetryBackoffMs()).isEqualTo(300L);
      assertThat(s3ClientConfig.getRetryMaxBackoffMs()).isEqualTo(20000L);
    }
  }
}
