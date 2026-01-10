package connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class S3StorageConfigureTest {

  @Test
  @DisplayName("성공: 필수 설정(버킷)이 있으면 초기화에 성공한다")
  void configureSuccess() {
    // Given
    S3Storage storage = new S3Storage();
    Map<String, String> configs = new HashMap<>();
    configs.put("claimcheck.s3.bucket.name", "test-bucket");
    configs.put("claimcheck.s3.region", "ap-northeast-1");

    // When
    storage.configure(configs);

    // Then
    assertEquals("test-bucket", storage.getBucketName());
    assertEquals("ap-northeast-1", storage.getRegion());
    assertNull(storage.getEndpointOverride());
  }

  @Test
  @DisplayName("성공: 엔드포인트 오버라이드 설정이 정상적으로 파싱된다")
  void configureWithEndpoint() {
    // Given
    S3Storage storage = new S3Storage();
    Map<String, String> configs = new HashMap<>();
    configs.put("claimcheck.s3.bucket.name", "test-bucket");
    configs.put("claimcheck.s3.endpoint.override", "http://localhost:4566");

    // When
    storage.configure(configs);

    // Then
    assertEquals("http://localhost:4566", storage.getEndpointOverride());
  }

  @Test
  @DisplayName("실패: 버킷 이름이 없으면 예외(ConfigException)가 발생해야 한다")
  void configureFailNoBucket() {
    // Given
    S3Storage storage = new S3Storage();
    Map<String, String> configs = new HashMap<>();
    configs.put("claimcheck.s3.region", "ap-northeast-2");

    // When & Then
    ConfigException exception =
        assertThrows(ConfigException.class, () -> storage.configure(configs));
    assertEquals(
        "Missing required configuration \"claimcheck.s3.bucket.name\" which has no default value.",
        exception.getMessage());
  }
}
