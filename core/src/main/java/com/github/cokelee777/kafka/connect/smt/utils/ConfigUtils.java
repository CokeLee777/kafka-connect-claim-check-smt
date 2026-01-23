package com.github.cokelee777.kafka.connect.smt.utils;

public class ConfigUtils {

  private ConfigUtils() {}

  public static String normalizePathPrefix(String prefix) {
    if (prefix == null) {
      return null;
    }

    String normalizedPrefix = prefix.trim();
    normalizedPrefix = normalizedPrefix.replaceAll("/+", "/");

    while (normalizedPrefix.startsWith("/")) {
      normalizedPrefix = normalizedPrefix.substring(1);
    }

    while (normalizedPrefix.endsWith("/")) {
      normalizedPrefix = normalizedPrefix.substring(0, normalizedPrefix.length() - 1);
    }
    return normalizedPrefix;
  }
}
