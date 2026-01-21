package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

public interface RetryStrategyFactory<T> {

  T create(RetryConfig config);
}
