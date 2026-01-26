package com.github.cokelee777.kafka.connect.smt.common.retry;

import java.time.Duration;
import java.util.Objects;

/** Configuration for retry behavior with exponential backoff. */
public class RetryConfig {

  private final int maxAttempts;
  private final Duration initialBackoff;
  private final Duration maxBackoff;

  /**
   * Creates a retry configuration.
   *
   * @param maxAttempts maximum number of retry attempts
   * @param initialBackoff initial backoff duration
   * @param maxBackoff maximum backoff duration
   */
  public RetryConfig(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    if (maxAttempts < 0) {
      throw new IllegalArgumentException("maxAttempts must be >= 0");
    }
    this.maxAttempts = maxAttempts;

    this.initialBackoff = Objects.requireNonNull(initialBackoff, "initialBackoff must not be null");
    if (initialBackoff().isZero() || initialBackoff.isNegative()) {
      throw new IllegalArgumentException("initialBackoff must be > 0");
    }

    this.maxBackoff = Objects.requireNonNull(maxBackoff, "maxBackoff must not be null");
    if (maxBackoff.isZero() || maxBackoff.isNegative()) {
      throw new IllegalArgumentException("maxBackoff must be > 0");
    }
  }

  /** Returns the maximum number of retry attempts. */
  public int maxAttempts() {
    return maxAttempts;
  }

  /** Returns the initial backoff duration. */
  public Duration initialBackoff() {
    return initialBackoff;
  }

  /** Returns the maximum backoff duration. */
  public Duration maxBackoff() {
    return maxBackoff;
  }
}
