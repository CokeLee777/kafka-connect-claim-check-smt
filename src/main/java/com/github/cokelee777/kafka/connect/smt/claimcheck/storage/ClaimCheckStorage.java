package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import java.util.Map;

public interface ClaimCheckStorage {

  void configure(Map<String, ?> configs);

  String store(byte[] data);

  void close();
}
