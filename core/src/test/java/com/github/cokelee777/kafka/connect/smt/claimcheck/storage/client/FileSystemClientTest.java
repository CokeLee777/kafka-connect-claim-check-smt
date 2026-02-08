package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FileSystemClientTest {

  private FileSystemClient fileSystemClient;
  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    RetryConfig retryConfig =
        new RetryConfig(
            FileSystemStorageConfig.RETRY_MAX_DEFAULT + 1,
            Duration.ofMillis(FileSystemStorageConfig.RETRY_BACKOFF_MS_DEFAULT),
            Duration.ofMillis(FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_DEFAULT));
    fileSystemClient = new FileSystemClient(retryConfig);
  }

  @Nested
  class WriteTest {

    @Test
    void shouldWritePayloadToFile() {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

      // When & Then
      assertThatNoException().isThrownBy(() -> fileSystemClient.write(path, payload));
    }

    @Test
    void shouldRetryOnRetriableException() {
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
        // Given
        Path path = tempDir.resolve("test.txt");
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenThrow(new IOException("retriable"))
            .thenAnswer(invocation -> null);

        // When
        assertThatNoException().isThrownBy(() -> fileSystemClient.write(path, payload));

        // Then
        mockedFiles.verify(() -> Files.write(path, payload), times(2));
      }
    }

    @Test
    void shouldNotRetryOnNonRetriableException() {
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
        // Given
        Path path = tempDir.resolve("test.txt");
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenThrow(new NoSuchFileException("non-retriable"));

        // When & Then
        assertThatExceptionOfType(NoSuchFileException.class)
            .isThrownBy(() -> fileSystemClient.write(path, payload))
            .withMessage("non-retriable");
        mockedFiles.verify(() -> Files.write(path, payload), times(1));
      }
    }
  }

  @Nested
  class ReadTest {

    @Test
    void shouldReadPayloadFromFile() throws IOException {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      fileSystemClient.write(path, payload);

      // When
      byte[] readPayload = fileSystemClient.read(path);

      // Then
      assertThat(readPayload).isEqualTo(payload);
    }

    @Test
    void shouldRetryOnRetriableException() {
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
        // Given
        Path path = tempDir.resolve("test.txt");
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        mockedFiles
            .when(() -> Files.readAllBytes(path))
            .thenThrow(new IOException("retriable"))
            .thenReturn(payload);

        // When & Then
        assertThatNoException()
            .isThrownBy(
                () -> {
                  byte[] readPayload = fileSystemClient.read(path);
                  assertThat(readPayload).isEqualTo(payload);
                });
        mockedFiles.verify(() -> Files.readAllBytes(path), times(2));
      }
    }

    @Test
    void shouldNotRetryOnNonRetriableException() {
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
        // Given
        Path path = tempDir.resolve("test.txt");
        mockedFiles
            .when(() -> Files.readAllBytes(path))
            .thenThrow(new NoSuchFileException("non-retriable"));

        // When & Then
        assertThatThrownBy(() -> fileSystemClient.read(path))
            .isInstanceOf(NoSuchFileException.class);
        mockedFiles.verify(() -> Files.readAllBytes(path), times(1));
      }
    }
  }
}
