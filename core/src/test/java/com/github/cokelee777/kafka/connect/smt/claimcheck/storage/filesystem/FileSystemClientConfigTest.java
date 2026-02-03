package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.FileSystemStorage;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("FileSystemClientConfig 단위 테스트")
class FileSystemClientConfigTest {

  @TempDir Path tempDir;

  @Test
  @DisplayName("올바른 설정정보를 세팅하면 정상적으로 FileSystemClientConfig가 생성된다.")
  void rightConfig() {
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

    // When
    FileSystemClientConfig fileSystemClientConfig =
        FileSystemStorage.Config.toFileSystemClientConfig(config);

    // Then
    assertThat(fileSystemClientConfig).isNotNull();
    assertThat(fileSystemClientConfig.retryMax()).isEqualTo(5);
    assertThat(fileSystemClientConfig.retryBackoffMs()).isEqualTo(500L);
    assertThat(fileSystemClientConfig.retryMaxBackoffMs()).isEqualTo(30000L);
  }
}
