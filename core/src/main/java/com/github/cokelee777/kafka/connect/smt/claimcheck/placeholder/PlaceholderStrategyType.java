package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder;

/** Supported default value strategy types. */
public enum PlaceholderStrategyType {
  SCHEMALESS("schemaless"),
  DEBEZIUM_STRUCT("debezium_struct"),
  GENERIC_STRUCT("generic_struct");

  private final String type;

  PlaceholderStrategyType(String type) {
    this.type = type;
  }

  /** Returns the type identifier string. */
  public String type() {
    return type;
  }
}
