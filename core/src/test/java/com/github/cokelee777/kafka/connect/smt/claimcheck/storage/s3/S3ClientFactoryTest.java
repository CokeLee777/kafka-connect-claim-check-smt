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
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3ClientFactory 단위 테스트")
class S3ClientFactoryTest {

  @Mock private SimpleConfig config;

  @Nested
  @DisplayName("create 메서드 테스트")
  class Create {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 S3Client가 생성된다.")
    public void normalConfig() {
      // Given
      when(config.getString(S3Storage.Config.REGION)).thenReturn("ap-northeast-2");
      when(config.getString(S3Storage.Config.ENDPOINT_OVERRIDE)).thenReturn(null);
      when(config.getInt(S3Storage.Config.RETRY_MAX)).thenReturn(3);
      when(config.getLong(S3Storage.Config.RETRY_BACKOFF_MS)).thenReturn(300L);
      when(config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS)).thenReturn(20000L);

      S3ClientFactory factory = new S3ClientFactory(config);

      // When
      S3Client s3Client = factory.create();

      // Then
      assertThat(s3Client).isNotNull();
      verify(config, times(1)).getString(S3Storage.Config.REGION);
      verify(config, times(1)).getString(S3Storage.Config.ENDPOINT_OVERRIDE);
      verify(config, times(1)).getInt(S3Storage.Config.RETRY_MAX);
      verify(config, times(1)).getLong(S3Storage.Config.RETRY_BACKOFF_MS);
      verify(config, times(1)).getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);
    }

    @Test
    @DisplayName("endpointOverride가 설정되면 forcePathStyle이 활성화된다.")
    public void withEndpointOverride() {
      // Given
      when(config.getString(S3Storage.Config.REGION)).thenReturn("ap-northeast-2");
      when(config.getString(S3Storage.Config.ENDPOINT_OVERRIDE))
          .thenReturn("http://localhost:4566");
      when(config.getInt(S3Storage.Config.RETRY_MAX)).thenReturn(3);
      when(config.getLong(S3Storage.Config.RETRY_BACKOFF_MS)).thenReturn(300L);
      when(config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS)).thenReturn(20000L);

      S3ClientFactory factory = new S3ClientFactory(config);

      // When
      S3Client s3Client = factory.create();

      // Then
      assertThat(s3Client).isNotNull();
      verify(config, times(1)).getString(S3Storage.Config.REGION);
      verify(config, times(1)).getString(S3Storage.Config.ENDPOINT_OVERRIDE);
      verify(config, times(1)).getInt(S3Storage.Config.RETRY_MAX);
      verify(config, times(1)).getLong(S3Storage.Config.RETRY_BACKOFF_MS);
      verify(config, times(1)).getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);
    }
  }

  @Nested
  @DisplayName("createOverrideConfiguration 메서드 테스트")
  class CreateOverrideConfiguration {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 ClientOverrideConfiguration가 생성된다.")
    public void normalConfig() {
      // Given
      when(config.getInt(S3Storage.Config.RETRY_MAX)).thenReturn(3);
      when(config.getLong(S3Storage.Config.RETRY_BACKOFF_MS)).thenReturn(300L);
      when(config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS)).thenReturn(20000L);

      S3ClientFactory factory = new S3ClientFactory(config);

      // When
      ClientOverrideConfiguration result = factory.createOverrideConfiguration();

      // Then
      assertThat(result).isNotNull();
      verify(config, times(1)).getInt(S3Storage.Config.RETRY_MAX);
      verify(config, times(1)).getLong(S3Storage.Config.RETRY_BACKOFF_MS);
      verify(config, times(1)).getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);
    }
  }
}
