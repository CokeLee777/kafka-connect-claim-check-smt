package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import java.util.Map;

/**
 * An interface for a storage backend used in the Claim Check pattern.
 *
 * <p>Implementations of this interface are responsible for configuring themselves and storing byte
 * array payloads in a specific backend (e.g., S3, GCS, etc.).
 */
public interface ClaimCheckStorage extends AutoCloseable {

  /**
   * Returns the type of the storage backend (e.g., "s3").
   *
   * @return The storage type identifier.
   */
  String type();

  /**
   * Configures the storage backend with the given properties.
   *
   * @param configs The configuration map.
   */
  void configure(Map<String, ?> configs);

  /**
   * Stores the given payload in the backend.
   *
   * @param payload The byte array payload to store.
   * @return A reference string or URL that can be used to retrieve the payload later.
   */
  String store(byte[] payload);

  /** Closes any open resources, such as network clients. */
  @Override
  void close();
}
