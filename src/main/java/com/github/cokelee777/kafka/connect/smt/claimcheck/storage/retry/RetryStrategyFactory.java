package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

/**
 * A factory for creating retry strategy instances.
 *
 * @param <T> The type of the retry strategy to be created (e.g., {@code StandardRetryStrategy}).
 */
public interface RetryStrategyFactory<T> {

  /**
   * Creates a new retry strategy instance based on the provided configuration.
   *
   * @param config The configuration for the retry strategy.
   * @return A new instance of the retry strategy.
   */
  T create(RetryConfig config);
}
