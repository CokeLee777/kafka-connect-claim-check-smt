package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("FileSystemClient 단위 테스트")
class FileSystemClientTest {

  private FileSystemClient fileSystemClient;

  @TempDir Path tempDir;

  @BeforeEach
  void beforeEach() {
    RetryConfig retryConfig = new RetryConfig(3, Duration.ofMillis(100), Duration.ofMillis(1000));
    fileSystemClient = new FileSystemClient(retryConfig);
  }

  @Test
  @DisplayName("쓰기 작업이 정상적으로 완료되어야 한다")
  void writeShouldCompleteSuccessfully() {
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] payload = "test data".getBytes();

      // When & Then
      assertThatNoException().isThrownBy(() -> fileSystemClient.write(path, payload));
      mockedFiles.verify(() -> Files.write(path, payload), times(1));
    }
  }

  @Test
  @DisplayName("쓰기 작업 중 재시도 가능한 예외가 발생하면 재시도해야 한다")
  void writeShouldRetryOnRetriableException() {
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] payload = "test data".getBytes();
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
  @DisplayName("쓰기 작업 중 재시도 불가능한 예외가 발생하면 즉시 실패해야 한다")
  void writeShouldFailImmediatelyOnNonRetriableException() {
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] payload = "test data".getBytes();
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

  @Test
  @DisplayName("읽기 작업이 정상적으로 완료되어야 한다")
  void readShouldCompleteSuccessfully() throws IOException {
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] expectedPayload = "test data".getBytes();
      mockedFiles.when(() -> Files.readAllBytes(path)).thenReturn(expectedPayload);

      // When
      byte[] actualPayload = fileSystemClient.read(path);

      // Then
      assertThat(actualPayload).isEqualTo(expectedPayload);
      mockedFiles.verify(() -> Files.readAllBytes(path), times(1));
    }
  }

  @Test
  @DisplayName("읽기 작업 중 재시도 가능한 예외가 발생하면 재시도해야 한다")
  void readShouldRetryOnRetriableException() {
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      // Given
      Path path = tempDir.resolve("test.txt");
      byte[] expectedPayload = "test data".getBytes();
      mockedFiles
          .when(() -> Files.readAllBytes(path))
          .thenThrow(new IOException("retriable"))
          .thenReturn(expectedPayload);

      // When & Then
      assertThatNoException()
          .isThrownBy(
              () -> {
                byte[] actualPayload = fileSystemClient.read(path);
                assertThat(actualPayload).isEqualTo(expectedPayload);
              });
      mockedFiles.verify(() -> Files.readAllBytes(path), times(2));
    }
  }

  @Test
  @DisplayName("읽기 작업 중 재시도 불가능한 예외가 발생하면 즉시 실패해야 한다")
  void readShouldFailImmediatelyOnNonRetriableException() {
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      // Given
      Path path = tempDir.resolve("test.txt");
      mockedFiles
          .when(() -> Files.readAllBytes(path))
          .thenThrow(new NoSuchFileException("non-retriable"));

      // When & Then
      assertThatThrownBy(() -> fileSystemClient.read(path)).isInstanceOf(NoSuchFileException.class);
      mockedFiles.verify(() -> Files.readAllBytes(path), times(1));
    }
  }
}
