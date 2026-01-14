package com.github.cokelee777.kafka.connect.smt.utils;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

public class ConfigUtils {

  private ConfigUtils() {}

  public static String getRequiredString(AbstractConfig config, String key) {
    String value = config.getString(key);
    if (value == null || value.isBlank()) {
      throw new ConfigException("Configuration \"" + key + "\" must not be empty or blank.");
    }
    return value.trim();
  }

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
