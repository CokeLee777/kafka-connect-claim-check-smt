package com.github.cokelee777.kafka.connect.smt.common.utils;

/** Utility methods for path string manipulation. */
public class PathUtils {

  private PathUtils() {}

  /**
   * Normalizes a path prefix by removing leading/trailing slashes and collapsing duplicates.
   *
   * @param prefix the path prefix to normalize
   * @return the normalized prefix, or {@code null} if input is {@code null}
   */
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
