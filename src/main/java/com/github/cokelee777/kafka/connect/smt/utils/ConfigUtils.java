package com.github.cokelee777.kafka.connect.smt.utils;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

/** Utility methods for handling Kafka Connect configurations. */
public class ConfigUtils {

  private ConfigUtils() {}

  /**
   * Gets a required string configuration value, ensuring it is not null, empty, or blank.
   *
   * @param config The configuration object.
   * @param key The configuration key.
   * @return The trimmed configuration value.
   * @throws ConfigException if the value is missing or blank.
   */
  public static String getRequiredString(AbstractConfig config, String key) {
    String value = config.getString(key);
    if (value == null || value.isBlank()) {
      throw new ConfigException("Configuration \"" + key + "\" must not be empty or blank.");
    }
    return value.trim();
  }

  /**
   * Gets an optional string configuration value. If present, ensures it is not empty or blank.
   *
   * @param config The configuration object.
   * @param key The configuration key.
   * @return The trimmed configuration value, or {@code null} if not present.
   * @throws ConfigException if the value is present but blank.
   */
  public static String getOptionalString(AbstractConfig config, String key) {
    String value = config.getString(key);
    if (value != null) {
      if (value.isBlank()) {
        throw new ConfigException(
            "Configuration \"" + key + "\" must not be empty or blank if provided.");
      }
      return value.trim();
    }
    return null;
  }

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
