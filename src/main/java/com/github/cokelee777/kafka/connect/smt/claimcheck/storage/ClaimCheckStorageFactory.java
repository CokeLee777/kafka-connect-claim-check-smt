package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.kafka.common.config.ConfigException;

/**
 * A factory for creating {@link ClaimCheckStorage} instances based on a type string.
 *
 * <p>This factory uses Java's {@link ServiceLoader} mechanism to discover available {@code
 * ClaimCheckStorage} implementations on the classpath.
 */
public class ClaimCheckStorageFactory {

  private static final Map<String, Class<? extends ClaimCheckStorage>> STORAGE_MAP =
      new HashMap<>();

  static {
    ServiceLoader.load(ClaimCheckStorage.class)
        .forEach(
            storage -> {
              String type = storage.type();
              if (type == null || type.isBlank()) {
                throw new ConfigException(
                    "Storage type must be non-empty: " + storage.getClass().getName());
              }
              STORAGE_MAP.put(type.toLowerCase(Locale.ROOT), storage.getClass());
            });
  }

  /**
   * Creates a new {@link ClaimCheckStorage} instance for the given type.
   *
   * @param type The storage type identifier (e.g., "s3").
   * @return A new, unconfigured {@link ClaimCheckStorage} instance.
   * @throws ConfigException if the requested storage type is not found.
   */
  public static ClaimCheckStorage create(String type) {
    if (type == null || type.isBlank()) {
      throw new ConfigException("Storage type must be provided");
    }

    Class<? extends ClaimCheckStorage> storageClass =
        STORAGE_MAP.get(type.toLowerCase(Locale.ROOT));
    if (storageClass == null) {
      throw new ConfigException("Unsupported storage type: " + type);
    }

    try {
      return storageClass.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new ConfigException("Failed to instantiate storage type: " + type, e);
    }
  }
}
