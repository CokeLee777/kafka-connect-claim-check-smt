package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.FileSystemStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.S3Storage;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class ClaimCheckStorageFactoryTest {

  @Nested
  class CreateTest {

    @Test
    void shouldCreateS3Storage() {
      // Given
      String type = ClaimCheckStorageType.S3.type();

      // When
      ClaimCheckStorage claimCheckStorage = ClaimCheckStorageFactory.create(type);

      // Then
      assertThat(claimCheckStorage).isNotNull();
      assertThat(claimCheckStorage).isInstanceOf(S3Storage.class);
      assertThat(claimCheckStorage.type()).isEqualTo(type);
    }

    @Test
    void shouldCreateFileSystemStorage() {
      // Given
      String type = ClaimCheckStorageType.FILESYSTEM.type();

      // When
      ClaimCheckStorage claimCheckStorage = ClaimCheckStorageFactory.create(type);

      // Then
      assertThat(claimCheckStorage).isNotNull();
      assertThat(claimCheckStorage).isInstanceOf(FileSystemStorage.class);
      assertThat(claimCheckStorage.type()).isEqualTo(type);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unsupportedType"})
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenStorageTypeIsUnsupported(String type) {
      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> ClaimCheckStorageFactory.create(type))
          .withMessage("Unsupported storage type: " + type);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenStorageTypeIsBlank(String type) {
      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> ClaimCheckStorageFactory.create(type))
          .withMessage("Storage type must be provided");
    }
  }
}
