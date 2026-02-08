package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.FileSystemStorageTestConfigProvider;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class FileSystemStorageTest {

  private FileSystemStorage fileSystemStorage;
  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    fileSystemStorage = new FileSystemStorage();
  }

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
  class ConfigureTest {

    @Test
    void shouldConfigureWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          FileSystemStorageTestConfigProvider.builder()
              .path(tempDir.toString())
              .retryMax(5)
              .retryBackoffMs(500L)
              .retryMaxBackoffMs(30000L)
              .build();

      // When
      fileSystemStorage.configure(configs);

      // Then
      assertThat(fileSystemStorage.getConfig().getPath()).isEqualTo(tempDir.toString());
      assertThat(fileSystemStorage.getConfig().getRetryMax()).isEqualTo(5);
      assertThat(fileSystemStorage.getConfig().getRetryBackoffMs()).isEqualTo(500L);
      assertThat(fileSystemStorage.getConfig().getRetryMaxBackoffMs()).isEqualTo(30000L);
    }

    @Test
    void shouldUseDefaultValuesWhenNoArgumentsProvided() {
      // Given
      Map<String, String> configs = FileSystemStorageTestConfigProvider.builder().build();

      // When
      fileSystemStorage.configure(configs);

      // Then
      assertThat(fileSystemStorage.getConfig().getPath())
          .isEqualTo(FileSystemStorageConfig.PATH_DEFAULT);

      Path path = Path.of(FileSystemStorageConfig.PATH_DEFAULT);
      Path normalizedPath = path.toAbsolutePath().normalize();
      assertThat(fileSystemStorage.getConfig().getNormalizedAbsolutePath())
          .isEqualTo(normalizedPath);

      assertThat(fileSystemStorage.getConfig().getRetryMax())
          .isEqualTo(FileSystemStorageConfig.RETRY_MAX_DEFAULT);
      assertThat(fileSystemStorage.getConfig().getRetryBackoffMs())
          .isEqualTo(FileSystemStorageConfig.RETRY_BACKOFF_MS_DEFAULT);
      assertThat(fileSystemStorage.getConfig().getRetryMaxBackoffMs())
          .isEqualTo(FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_DEFAULT);
    }

    @Test
    void shouldThrowExceptionWhenPathIsFile() throws IOException {
      // Given
      Path filePath = tempDir.resolve("test.txt").toAbsolutePath().normalize();
      Files.createFile(filePath);
      Map<String, String> configs =
          FileSystemStorageTestConfigProvider.builder().path(filePath.toString()).build();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> fileSystemStorage.configure(configs))
          .withMessage("Storage path exists but is not a directory: " + filePath.toRealPath());
    }

    @Test
    void shouldThrowExceptionWhenPathIsNotWritable() throws IOException {
      // Given
      File readOnlyDir = tempDir.resolve("read-only").toFile();
      readOnlyDir.mkdir();
      readOnlyDir.setReadOnly();

      Map<String, String> configs =
          FileSystemStorageTestConfigProvider.builder().path(readOnlyDir.toString()).build();

      // When & Then
      try {
        assertThatExceptionOfType(ConfigException.class)
            .isThrownBy(() -> fileSystemStorage.configure(configs))
            .withMessage(
                "Storage directory is not writable: "
                    + readOnlyDir.toPath().toAbsolutePath().normalize().toRealPath());
      } finally {
        // Clean up
        readOnlyDir.setWritable(true);
      }
    }
  }

  @Nested
  class StoreTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs =
          FileSystemStorageTestConfigProvider.builder().path(tempDir.toString()).build();
      fileSystemStorage.configure(configs);
    }

    @Test
    void shouldStorePayloadAndReturnFileReferenceUrl() throws IOException {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      String referenceUrl = fileSystemStorage.store(payload);

      // Then
      String filePathPrefix = "file://";
      assertThat(referenceUrl).startsWith(filePathPrefix);

      Path path = Path.of(referenceUrl.substring(filePathPrefix.length()));
      assertThat(path).exists();
      assertThat(Files.readAllBytes(path)).isEqualTo(payload);
    }
  }

  @Nested
  class RetrieveTest {

    @BeforeEach
    void setUp() {
      Map<String, String> configs =
          FileSystemStorageTestConfigProvider.builder().path(tempDir.toString()).build();
      fileSystemStorage.configure(configs);
    }

    @Test
    void shouldRetrieveStoredPayload() {
      // Given
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      String referenceUrl = fileSystemStorage.store(payload);

      // When
      byte[] retrievedPayload = fileSystemStorage.retrieve(referenceUrl);

      // Then
      assertThat(retrievedPayload).isEqualTo(payload);
    }

    @Test
    void shouldThrowExceptionWhenFileDoesNotExist() {
      // Given
      String referenceUrl = "file://" + tempDir.resolve("non_existent_file.txt");

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> fileSystemStorage.retrieve(referenceUrl))
          .withMessageStartingWith("Claim check file does not exist or cannot be accessed:");
    }

    @Test
    void shouldThrowExceptionWhenFileIsOutsideStoragePath() throws IOException {
      // Given
      Path outsideFile = Files.createTempFile("outside", ".txt");
      String referenceUrl = "file://" + outsideFile.toAbsolutePath();

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> fileSystemStorage.retrieve(referenceUrl))
          .withMessageContaining("is outside the configured storage path");

      // Clean up
      Files.deleteIfExists(outsideFile);
    }

    @Test
    void shouldThrowExceptionWhenUrlSchemeIsInvalid() {
      // Given
      String invalidUrl = "s3://" + tempDir.resolve("some_file.txt");

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> fileSystemStorage.retrieve(invalidUrl))
          .withMessage("File reference URL must start with 'file://'");
    }
  }
}
