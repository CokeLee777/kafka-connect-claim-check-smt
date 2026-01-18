package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;

/**
 * A factory for creating {@link ClaimCheckStorage} instances based on a type string.
 *
 * <p>This factory maintains an internal registry of supported storage implementations and creates
 * new instances on demand using {@link Supplier suppliers}.
 *
 * <p>The registry is intentionally explicit to ensure predictable behavior in Kafka Connect plugin
 * environments where classloader isolation is applied.
 */
public class ClaimCheckStorageFactory {

  private static final Map<String, Supplier<ClaimCheckStorage>> STORAGE_MAP = new HashMap<>();

  static {
    register("s3", S3Storage::new);
  }

  /**
   * Registers a {@link ClaimCheckStorage} supplier for the given storage type.
   *
   * <p>The type is normalized to lower-case using {@link Locale#ROOT} to ensure case-insensitive
   * lookup.
   *
   * @param type the storage type identifier (e.g., {@code "s3"})
   * @param supplier a supplier that creates new {@link ClaimCheckStorage} instances
   */
  private static void register(String type, Supplier<ClaimCheckStorage> supplier) {
    STORAGE_MAP.put(type.toLowerCase(Locale.ROOT), supplier);
  }

  /**
   * Creates a new {@link ClaimCheckStorage} instance for the given type.
   *
   * <p>A new instance is created for each invocation.
   *
   * @param type The storage type identifier (e.g., "s3").
   * @return A new, unconfigured {@link ClaimCheckStorage} instance.
   * @throws ConfigException if the requested storage type is not found.
   */
  public static ClaimCheckStorage create(String type) {
    if (type == null || type.isBlank()) {
      throw new ConfigException("Storage type must be provided");
    }

    Supplier<ClaimCheckStorage> storageSupplier = STORAGE_MAP.get(type.toLowerCase(Locale.ROOT));
    if (storageSupplier == null) {
      throw new ConfigException("Unsupported storage type: " + type);
    }

    return storageSupplier.get();
  }
}
