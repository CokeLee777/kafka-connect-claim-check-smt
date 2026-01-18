package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

/** Enumerates the supported types of storage backends. */
public enum StorageType {
  /** Amazon S3 storage. */
  S3("s3");

  private final String type;

  StorageType(String type) {
    this.type = type;
  }

  /**
   * Returns the string identifier for the storage type.
   *
   * @return The lower-case type string (e.g., "s3").
   */
  public String type() {
    return type;
  }

  /**
   * Finds a {@link StorageType} enum constant from a string identifier.
   *
   * @param value The string to match (case-insensitive).
   * @return The corresponding {@link StorageType}.
   * @throws IllegalArgumentException if no matching storage type is found.
   */
  public static StorageType from(String value) {
    for (StorageType storageType : values()) {
      if (storageType.type.equalsIgnoreCase(value)) {
        return storageType;
      }
    }
    throw new IllegalArgumentException("Unknown storage type: " + value);
  }
}
