package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import java.util.Map;

/**
 * Storage backend for the Claim Check pattern.
 *
 * <p>Stores large payloads externally and returns a reference URL for retrieval.
 */
public sealed interface ClaimCheckStorage
    permits CloseableClaimCheckStorage, FileSystemStorage {

  /**
   * Returns the storage type identifier.
   *
   * @return the type name (e.g., "s3")
   */
  String type();

  /**
   * Configures the storage with the provided settings.
   *
   * @param configs configuration properties
   */
  void configure(Map<String, ?> configs);

  /**
   * Stores the payload and returns a reference URL.
   *
   * @param payload the data to store
   * @return the reference URL for retrieval
   */
  String store(byte[] payload);

  /**
   * Retrieves the payload from the given reference URL.
   *
   * @param referenceUrl the reference URL returned by {@link #store(byte[])}
   * @return the stored payload
   */
  byte[] retrieve(String referenceUrl);
}
