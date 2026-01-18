package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

public enum StorageType {
  S3("s3");

  private final String type;

  StorageType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }

  public static StorageType from(String value) {
    for (StorageType storageType : values()) {
      if (storageType.type.equalsIgnoreCase(value)) {
        return storageType;
      }
    }
    throw new IllegalArgumentException("Unknown storage type: " + value);
  }
}
