package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

import java.time.Duration;
import java.util.Objects;

/** Holds the configuration for a retry mechanism. */
public class RetryConfig {

  private final int maxAttempts;
  private final Duration initialBackoff;
  private final Duration maxBackoff;

  /**
   * Constructs a new RetryConfig.
   *
   * @param maxAttempts The maximum number of attempts to be made.
   * @param initialBackoff The initial duration to wait before the first retry.
   * @param maxBackoff The maximum duration to wait between retries.
   */
  public RetryConfig(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1");
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

  public int maxAttempts() {
    return maxAttempts;
  }

  public Duration initialBackoff() {
    return initialBackoff;
  }

  public Duration maxBackoff() {
    return maxBackoff;
  }
}
