package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("RetryConfig 단위 테스트")
class RetryConfigTest {

  @Nested
  @DisplayName("생성자 테스트")
  class ConstructorTest {

    @Test
    @DisplayName("올바른 인자를 세팅하면 정상적으로 객체가 생성된다.")
    public void rightArgs() {
      // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When
      RetryConfig retryConfig = new RetryConfig(maxAttempts, initialBackoff, maxBackoff);

      // Then
      assertThat(retryConfig).isNotNull();
      assertThat(retryConfig.maxAttempts()).isSameAs(maxAttempts);
      assertThat(retryConfig.initialBackoff()).isSameAs(initialBackoff);
      assertThat(retryConfig.maxBackoff()).isSameAs(maxBackoff);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -2, -3})
    @DisplayName("maxAttempts를 0보다 낮게 설정하면 예외가 발생한다.")
    public void setMaxAttemptsLessThanZeroCauseException(int maxAttempts) { // Given
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .withMessage("maxAttempts must be >= 0");
    }

    @ParameterizedTest
    @ValueSource(longs = {-100L, -200L, -300L})
    @DisplayName("initialBackoff를 0보다 낮게 설정하면 예외가 발생한다.")
    public void setInitialBackoffLessThanZeroCauseException(long initialBackoffMs) {
      // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(initialBackoffMs);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .withMessage("initialBackoff must be > 0");
    }

    @ParameterizedTest
    @ValueSource(longs = {-10000L, -20000L, -30000L})
    @DisplayName("maxBackoff 0보다 낮게 설정하면 예외가 발생한다.")
    public void setMaxBackoffLessThanZeroCauseException(long maxBackoffMs) { // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(maxBackoffMs);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .withMessage("maxBackoff must be > 0");
    }
  }
}
