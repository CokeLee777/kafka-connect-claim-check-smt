package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryStrategyFactory;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;

/**
 * A factory for creating {@link StandardRetryStrategy} instances for the AWS S3 client based on a
 * generic {@link RetryConfig}.
 *
 * <p>This class adapts the SMT's retry configuration to the format expected by the AWS SDK,
 * specifically creating an exponential backoff strategy.
 */
public class S3RetryConfigAdapter implements RetryStrategyFactory<StandardRetryStrategy> {

  /**
   * Creates a {@link StandardRetryStrategy} for the AWS SDK.
   *
   * @param config The generic retry configuration.
   * @return A configured {@link StandardRetryStrategy} instance.
   */
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
