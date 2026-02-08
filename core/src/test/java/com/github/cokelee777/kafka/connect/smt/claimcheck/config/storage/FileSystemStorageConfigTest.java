package com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.FileSystemStorageTestConfigProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSystemStorageConfigTest {

  @AfterEach
  void tearDown() {
    try {
      Path path =
          Path.of(FileSystemStorageConfig.PATH_DEFAULT).toAbsolutePath().normalize().toRealPath();
      Files.deleteIfExists(path);
    } catch (IOException e) {
      // Ignore cleanup failure
    }
  }

  @Nested
  class ConstructorTest {

    @TempDir Path tempDir;

    @Test
    void shouldConstructWithAllProvidedArguments() {
      // Given
      String path = tempDir.toString();
      Map<String, String> configs =
          FileSystemStorageTestConfigProvider.builder()
              .path(path)
              .retryMax(5)
              .retryBackoffMs(500L)
              .retryMaxBackoffMs(30000L)
              .build();

      // When
      FileSystemStorageConfig config = new FileSystemStorageConfig(configs);

      // Then
      assertThat(config.getPath()).isEqualTo(path);
      assertThat(config.getNormalizedAbsolutePath())
          .isEqualTo(Path.of(path).toAbsolutePath().normalize());
      assertThat(config.getRetryMax()).isEqualTo(5);
      assertThat(config.getRetryBackoffMs()).isEqualTo(500L);
      assertThat(config.getRetryMaxBackoffMs()).isEqualTo(30000L);
    }

    @Test
    void shouldUseDefaultValuesWhenNoArgumentsProvided() {
      // Given
      Map<String, String> configs = FileSystemStorageTestConfigProvider.builder().build();

      // When
      FileSystemStorageConfig config = new FileSystemStorageConfig(configs);

      // Then
      Path normalizedAbsolutePath =
          Path.of(FileSystemStorageConfig.PATH_DEFAULT).toAbsolutePath().normalize();
      assertThat(config.getPath()).isEqualTo(FileSystemStorageConfig.PATH_DEFAULT);
      assertThat(config.getNormalizedAbsolutePath()).isEqualTo(normalizedAbsolutePath);

      assertThat(config.getRetryMax()).isEqualTo(RETRY_MAX_DEFAULT);
      assertThat(config.getRetryBackoffMs()).isEqualTo(RETRY_BACKOFF_MS_DEFAULT);
      assertThat(config.getRetryMaxBackoffMs()).isEqualTo(RETRY_MAX_BACKOFF_MS_DEFAULT);
    }
  }
}
