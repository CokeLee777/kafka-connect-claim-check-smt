package com.github.cokelee777.kafka.connect.smt.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AutoCloseableUtils {

  private static final Logger log = LoggerFactory.getLogger(AutoCloseableUtils.class);

  private AutoCloseableUtils() {}

  public static void closeQuietly(AutoCloseable autoCloseable) {
    if (autoCloseable == null) {
      return;
    }

    try {
      autoCloseable.close();
    } catch (InterruptedException e) {
      log.warn("Closing autoCloseable instance was interrupted", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.warn("Failed to close autoCloseable instance", e);
    }
  }
}
