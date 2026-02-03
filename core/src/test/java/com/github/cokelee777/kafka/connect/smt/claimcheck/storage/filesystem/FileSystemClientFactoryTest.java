package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.FileSystemStorage;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("FileSystemClientFactory 단위 테스트")
class FileSystemClientFactoryTest {

  private FileSystemClientFactory fileSystemClientFactory;

  @TempDir Path tempDir;

  @BeforeEach
  void beforeEach() {
    fileSystemClientFactory = new FileSystemClientFactory();
  }

  @Test
  @DisplayName("올바른 설정정보를 세팅하면 정상적으로 FileSystemClient가 생성된다.")
  void create() {
    // Given
    Map<String, ?> originals =
        Map.of(
            FileSystemStorage.Config.PATH,
            tempDir.toString(),
            FileSystemStorage.Config.RETRY_MAX,
            5,
            FileSystemStorage.Config.RETRY_BACKOFF_MS,
            500L,
            FileSystemStorage.Config.RETRY_MAX_BACKOFF_MS,
            30000L);
    SimpleConfig config = new SimpleConfig(FileSystemStorage.Config.DEFINITION, originals);
    FileSystemClientConfig fileSystemClientConfig =
        FileSystemStorage.Config.toFileSystemClientConfig(config);

    // When
    FileSystemClient fileSystemClient = fileSystemClientFactory.create(fileSystemClientConfig);

    // Then
    assertThat(fileSystemClient).isNotNull();
  }
}
