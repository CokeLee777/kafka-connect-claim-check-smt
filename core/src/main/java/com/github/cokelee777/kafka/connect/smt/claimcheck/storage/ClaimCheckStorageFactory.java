package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem.FileSystemStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;

/** Factory for creating {@link ClaimCheckStorage} instances. */
public class ClaimCheckStorageFactory {

  private static final Map<String, Supplier<ClaimCheckStorage>> STORAGE_MAP = new HashMap<>();

  static {
    register(ClaimCheckStorageType.S3.type(), S3Storage::new);
    register(ClaimCheckStorageType.FILESYSTEM.type(), FileSystemStorage::new);
  }

  private static void register(String type, Supplier<ClaimCheckStorage> supplier) {
    STORAGE_MAP.put(type.toLowerCase(Locale.ROOT), supplier);
  }

  /**
   * Creates a storage instance of the specified type.
   *
   * @param type the storage type (e.g., s3, filesystem)
   * @return a new ClaimCheckStorage instance
   * @throws ConfigException if the type is unsupported or blank
   */
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
