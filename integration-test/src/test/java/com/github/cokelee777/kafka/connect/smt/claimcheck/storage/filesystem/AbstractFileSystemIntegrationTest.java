package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractFileSystemIntegrationTest {

  private static final Logger log =
      LoggerFactory.getLogger(AbstractFileSystemIntegrationTest.class);

  protected static final String TOPIC_NAME = "test-topic";
  protected static Path TEMP_DIR_PATH;

  @BeforeAll
  static void beforeAll() throws IOException {
    TEMP_DIR_PATH = Files.createTempDirectory("file-system-claim-check-test");
  }

  @AfterAll
  static void afterAll() throws IOException {
    // Delete the temporary directory and its contents
    if (TEMP_DIR_PATH != null && Files.exists(TEMP_DIR_PATH)) {
      try (Stream<Path> pathStream = Files.walk(TEMP_DIR_PATH)) {
        pathStream
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    log.error("Failed to delete {}", path, e);
                  }
                });
      }
    }
  }
}
