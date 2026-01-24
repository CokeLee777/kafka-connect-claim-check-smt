package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

public enum ClaimCheckStorageType {
  S3("s3");

  private final String type;

  ClaimCheckStorageType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }
}
