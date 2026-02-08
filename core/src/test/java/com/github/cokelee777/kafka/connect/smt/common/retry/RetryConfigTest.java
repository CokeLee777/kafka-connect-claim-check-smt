package com.github.cokelee777.kafka.connect.smt.common.retry;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RetryConfigTest {

  @Nested
  class ConstructorTest {

    @Test
    void shouldCreateWithAllValidArguments() {
      // Given
      int maxAttempts = 3;
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When
      RetryConfig retryConfig = new RetryConfig(maxAttempts, initialBackoff, maxBackoff);

      // Then
      assertThat(retryConfig).isNotNull();
      assertThat(retryConfig.maxAttempts()).isEqualTo(maxAttempts);
      assertThat(retryConfig.initialBackoff()).isEqualTo(initialBackoff);
      assertThat(retryConfig.maxBackoff()).isEqualTo(maxBackoff);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -2, -3})
    void shouldThrowExceptionWhenMaxAttemptsIsNegative(int maxAttempts) {
      // Given
      Duration initialBackoff = Duration.ofMillis(300L);
      Duration maxBackoff = Duration.ofMillis(20000L);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new RetryConfig(maxAttempts, initialBackoff, maxBackoff))
          .withMessage("maxAttempts must be >= 0");
    }

    @ParameterizedTest
    @ValueSource(longs = {-100L, -200L, -300L})
    void shouldThrowExceptionWhenInitialBackoffIsNegative(long initialBackoffMs) {
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
    void shouldThrowExceptionWhenMaxBackoffIsNegative(long maxBackoffMs) {
      // Given
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
