package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

/** Supported external storage backend types. */
public enum ClaimCheckStorageType {
  S3("s3");

  private final String type;

  ClaimCheckStorageType(String type) {
    this.type = type;
  }

  /** Returns the type identifier string. */
  public String type() {
    return type;
  }
}
