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
}
