package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;

public class ClaimCheckStorageFactory {

  private static final Map<String, Supplier<ClaimCheckStorage>> STORAGE_MAP = new HashMap<>();

  static {
    register(ClaimCheckStorageType.S3.type(), S3Storage::new);
  }

  private static void register(String type, Supplier<ClaimCheckStorage> supplier) {
    STORAGE_MAP.put(type.toLowerCase(Locale.ROOT), supplier);
  }

  public static ClaimCheckStorage create(String type) {
    if (type == null || type.isBlank()) {
      throw new ConfigException("Storage type must be provided");
    }

    Supplier<ClaimCheckStorage> supplier = STORAGE_MAP.get(type.toLowerCase(Locale.ROOT));
    if (supplier == null) {
      throw new ConfigException("Unsupported storage type: " + type);
    }

    return supplier.get();
  }
}
