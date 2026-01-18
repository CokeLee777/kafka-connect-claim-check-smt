package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ClaimCheckStorageFactory 단위 테스트")
public class ClaimCheckStorageFactoryTest {

  private static final String TEST_STORAGE_TYPE = "s3";
  private static final String TEST_UNSUPPORTED_STORAGE_TYPE = "UnsupportedStorage";

  @Test
  @DisplayName("s3 타입이면 S3Storage를 생성한다")
  void shouldCreateS3Storage() {
    // when
    ClaimCheckStorage storage = ClaimCheckStorageFactory.create(TEST_STORAGE_TYPE);

    // then
    assertNotNull(storage);
    assertTrue(storage instanceof S3Storage);
    assertEquals(TEST_STORAGE_TYPE, storage.type());
  }

  @Test
  @DisplayName("지원하지 않는 타입이면 ConfigException이 발생한다")
  void shouldThrowExceptionForUnsupportedType() {
    // when & then
    ConfigException exception =
        assertThrows(
            ConfigException.class,
            () -> ClaimCheckStorageFactory.create(TEST_UNSUPPORTED_STORAGE_TYPE));

    assertEquals(
        "Unsupported storage type: " + TEST_UNSUPPORTED_STORAGE_TYPE, exception.getMessage());
  }
}
