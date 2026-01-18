package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class ClaimCheckStorageFactory {

  private static final Map<String, ClaimCheckStorage> STORAGE_MAP = new HashMap<>();

  static {
    ServiceLoader.load(ClaimCheckStorage.class)
        .forEach(storage -> STORAGE_MAP.put(storage.type().toLowerCase(), storage));
  }

  public static ClaimCheckStorage create(String type) {
    ClaimCheckStorage storage = STORAGE_MAP.get(type.toLowerCase());
    if (storage == null) {
      throw new ConfigException("Unsupported storage type: " + type);
    }
    return storage;
  }
}
