package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("FileSystemStorage 단위 테스트")
class FileSystemStorageTest {

  private FileSystemStorage fileSystemStorage;

  @TempDir Path tempDir;

  @BeforeEach
  void beforeEach() {
    fileSystemStorage = new FileSystemStorage();
  }

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTest {

    @Test
    @DisplayName("올바른 설정정보를 세팅하면 정상적으로 구성된다.")
    void rightConfig() throws IOException {
      // Given
      Map<String, String> configs = Map.of(FileSystemStorage.Config.PATH, tempDir.toString());

      // When
      fileSystemStorage.configure(configs);

      // Then
      assertThat(fileSystemStorage.getStoragePath()).isEqualTo(tempDir.toRealPath());
    }

    @Test
    @DisplayName("설정정보에 경로를 지정하지 않으면 기본 경로로 정상적으로 구성된다.")
    void defaultPathConfig() {
      // Given
      Map<String, String> configs = Collections.emptyMap();
      Path defaultPath = Path.of(FileSystemStorage.Config.DEFAULT_PATH).toAbsolutePath();

      // When
      fileSystemStorage.configure(configs);

      // Then
      assertThat(fileSystemStorage.getStoragePath()).isEqualTo(defaultPath);

      // Clean up the default directory
      try {
        Files.deleteIfExists(defaultPath);
      } catch (IOException e) {
        // Ignore cleanup failure
      }
    }

    @Test
    @DisplayName("설정 경로가 파일이면 예외가 발생한다.")
    void pathIsFileCauseException() throws IOException {
      // Given
      Path filePath = tempDir.resolve("test.txt");
      Files.createFile(filePath);
      Map<String, String> configs = Map.of(FileSystemStorage.Config.PATH, filePath.toString());

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> fileSystemStorage.configure(configs))
          .withMessage("Storage path exists but is not a directory: " + filePath);
    }

    @Test
    @DisplayName("설정 경로에 쓰기 권한이 없으면 예외가 발생한다.")
    void pathIsNotWritableCauseException() {
      // Given
      File readOnlyDir = tempDir.resolve("read-only").toFile();
      readOnlyDir.mkdir();
      readOnlyDir.setReadOnly();
      Assumptions.assumeTrue(!readOnlyDir.canWrite(), "bypass read-only permission");

      Map<String, String> configs =
          Map.of(FileSystemStorage.Config.PATH, readOnlyDir.getAbsolutePath());

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> fileSystemStorage.configure(configs))
          .withMessage("Storage directory is not writable: " + readOnlyDir.toPath());

      // Clean up
      readOnlyDir.setWritable(true);
    }
  }

  @Nested
  @DisplayName("store 와 retrieve 메서드 테스트")
  class StoreAndRetrieveTest {

    private final byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void beforeEach() {
      Map<String, String> configs = Map.of(FileSystemStorage.Config.PATH, tempDir.toString());
      fileSystemStorage.configure(configs);
    }

    @Test
    @DisplayName("정상적으로 페이로드를 저장하고 읽어온다.")
    void storeAndRetrieveSuccessfully() throws IOException {
      // When
      String referenceUrl = fileSystemStorage.store(payload);

      // Then
      assertThat(referenceUrl).startsWith("file://");
      Path storedPath = Path.of(referenceUrl.substring("file://".length()));
      assertThat(storedPath).exists();
      assertThat(Files.readAllBytes(storedPath)).isEqualTo(payload);

      byte[] retrievedPayload = fileSystemStorage.retrieve(referenceUrl);
      assertThat(retrievedPayload).isEqualTo(payload);
    }

    @Test
    @DisplayName("존재하지 않는 파일을 읽으려 하면 예외가 발생한다.")
    void retrieveNonExistentFileCauseException() {
      // Given
      String referenceUrl = "file://" + tempDir.resolve("non_existent_file.txt");

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> fileSystemStorage.retrieve(referenceUrl))
          .withMessageStartingWith("Claim check file does not exist or cannot be accessed:");
    }

    @Test
    @DisplayName("설정된 스토리지 경로 밖의 파일을 읽으려 하면 예외가 발생한다. (보안)")
    void retrieveFileOutsideStoragePathCauseException() throws IOException {
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
    @DisplayName("잘못된 형식의 URL을 사용하면 예외가 발생한다.")
    void retrieveWithInvalidUrlCauseException() {
      // Given
      String invalidUrl = "s3://" + tempDir.resolve("some_file.txt");

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> fileSystemStorage.retrieve(invalidUrl))
          .withMessage("File reference URL must start with 'file://'");
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTest {

    @Test
    @DisplayName("close 호출 시 예외가 발생하지 않는다.")
    void closeDoesNotThrowException() {
      // Given & When & Then
      assertDoesNotThrow(() -> fileSystemStorage.close());
    }
  }
}
