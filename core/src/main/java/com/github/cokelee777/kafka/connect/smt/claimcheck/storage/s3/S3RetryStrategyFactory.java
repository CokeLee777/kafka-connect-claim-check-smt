package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryStrategyFactory;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;

public class S3RetryStrategyFactory implements RetryStrategyFactory<StandardRetryStrategy> {

  @Override
  public StandardRetryStrategy create(RetryConfig config) {
    BackoffStrategy backoffStrategy =
        BackoffStrategy.exponentialDelay(config.initialBackoff(), config.maxBackoff());

    return StandardRetryStrategy.builder()
        .maxAttempts(config.maxAttempts())
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .circuitBreakerEnabled(false)
        .build();
  }
}
