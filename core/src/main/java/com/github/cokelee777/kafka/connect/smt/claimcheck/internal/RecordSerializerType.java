package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

public enum RecordSerializerType {
  JSON("json");

  private final String type;

  RecordSerializerType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }
}
