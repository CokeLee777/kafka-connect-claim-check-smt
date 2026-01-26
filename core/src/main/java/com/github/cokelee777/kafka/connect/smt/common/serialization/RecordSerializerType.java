package com.github.cokelee777.kafka.connect.smt.common.serialization;

/** Supported record serializer types. */
public enum RecordSerializerType {
  JSON("json");

  private final String type;

  RecordSerializerType(String type) {
    this.type = type;
  }

  /** Returns the type identifier string. */
  public String type() {
    return type;
  }
}
