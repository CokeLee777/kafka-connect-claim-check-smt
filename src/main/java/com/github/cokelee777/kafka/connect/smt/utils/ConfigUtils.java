package com.github.cokelee777.kafka.connect.smt.utils;

public class ConfigUtils {

  private ConfigUtils() {}

  public static String normalizePathPrefix(String prefix) {
    if (prefix == null) {
      return null;
    }

    String trimmedPrefix = prefix.trim();
    while (trimmedPrefix.endsWith("/")) {
      trimmedPrefix = trimmedPrefix.substring(0, trimmedPrefix.length() - 1);
    }
    return trimmedPrefix;
  }
}
