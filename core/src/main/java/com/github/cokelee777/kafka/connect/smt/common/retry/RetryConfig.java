package com.github.cokelee777.kafka.connect.smt.common.retry;

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigException;

/** Configuration for retry behavior with exponential backoff. */
public record RetryConfig(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {

  /**
   * Creates a retry configuration.
   *
   * @param maxAttempts maximum number of retry attempts
   * @param initialBackoff initial backoff duration
   * @param maxBackoff maximum backoff duration
   */
  public RetryConfig {
    if (maxAttempts < 0) {
      throw new ConfigException("maxAttempts must be >= 0, but was: " + maxAttempts);
    }

    Objects.requireNonNull(initialBackoff, "initialBackoff must not be null");
    if (initialBackoff.isZero() || initialBackoff.isNegative()) {
      throw new ConfigException("initialBackoff must be > 0");
    }

    Objects.requireNonNull(maxBackoff, "maxBackoff must not be null");
    if (maxBackoff.isZero() || maxBackoff.isNegative()) {
      throw new ConfigException("maxBackoff must be > 0");
    }

    if (initialBackoff.compareTo(maxBackoff) > 0) {
      throw new ConfigException(
          String.format(
              "initialBackoff must be <= maxBackoff, but initialBackoff=%s, maxBackoff=%s",
              initialBackoff, maxBackoff));
    }
  }
}
