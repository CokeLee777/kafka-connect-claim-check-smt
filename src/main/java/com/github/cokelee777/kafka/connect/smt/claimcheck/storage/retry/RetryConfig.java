package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

import java.time.Duration;

public class RetryConfig {

  private final int maxAttempts;
  private final Duration initialBackoff;
  private final Duration maxBackoff;

  public RetryConfig(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    this.maxAttempts = maxAttempts;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
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
