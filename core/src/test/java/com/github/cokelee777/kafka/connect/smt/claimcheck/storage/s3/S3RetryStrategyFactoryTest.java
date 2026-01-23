package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.retries.StandardRetryStrategy;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3RetryStrategyFactory 단위 테스트")
class S3RetryStrategyFactoryTest {

  @InjectMocks private S3RetryStrategyFactory factory;
  @Mock private RetryConfig config;

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 S3RetryStrategy가 생성된다.")
    public void rightConfig() {
      // Given
      when(config.maxAttempts()).thenReturn(4);
      when(config.initialBackoff()).thenReturn(Duration.ofMillis(300L));
      when(config.maxBackoff()).thenReturn(Duration.ofMillis(20000L));

      // When
      StandardRetryStrategy strategy = factory.create(config);

      // Then
      assertThat(strategy).isNotNull();
      assertThat(strategy.maxAttempts()).isEqualTo(4);
    }
  }
}
