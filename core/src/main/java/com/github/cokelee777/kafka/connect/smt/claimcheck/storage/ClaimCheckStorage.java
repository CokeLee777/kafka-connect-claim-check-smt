package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import java.util.Map;

public interface ClaimCheckStorage extends AutoCloseable {

  String type();

  void configure(Map<String, ?> configs);

  String store(byte[] payload);

  byte[] retrieve(String referenceUrl);

  @Override
  void close();
}
