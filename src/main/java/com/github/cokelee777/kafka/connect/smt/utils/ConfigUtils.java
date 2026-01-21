package com.github.cokelee777.kafka.connect.smt.utils;

/** Utility methods for handling Kafka Connect configurations. */
public class ConfigUtils {

  private ConfigUtils() {}

  /**
   * Normalizes a path prefix string by trimming it and removing any trailing slashes.
   *
   * @param prefix The path prefix to normalize.
   * @return The normalized path prefix, or {@code null} if the input was null.
   */
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
