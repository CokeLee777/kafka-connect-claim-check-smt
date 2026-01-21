package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3Storage 단위 테스트")
class S3StorageTest {

  private static final String TEST_CONFIG_BUCKET_NAME = "test-bucket";
  private static final String TEST_CONFIG_PATH_PREFIX = "my-prefix";

  private S3Storage storage;
  @Mock private S3Client s3Client;

  private Map<String, String> createDefaultConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(S3Storage.Config.BUCKET_NAME, TEST_CONFIG_BUCKET_NAME);
    configs.put(S3Storage.Config.PATH_PREFIX, TEST_CONFIG_PATH_PREFIX);
    return configs;
  }

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTests {

    @BeforeEach
    void setUp() {
      // Use the default constructor for configure tests
      storage = new S3Storage();
    }

    @Test
    @DisplayName("필수 설정(버킷)만 있어도 정상적으로 초기화된다")
    void shouldInitializeWithRequiredConfig() {
      // Given
      Map<String, String> configs = new HashMap<>();
      configs.put(S3Storage.Config.BUCKET_NAME, TEST_CONFIG_BUCKET_NAME);

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_CONFIG_BUCKET_NAME, storage.getBucketName());
      assertEquals("claim-checks", storage.getPathPrefix()); // Default value
    }

    @Test
    @DisplayName("모든 설정이 제공되면 정상적으로 초기화된다")
    void shouldInitializeWithAllConfigs() {
      // Given
      Map<String, String> configs = createDefaultConfigs();

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_CONFIG_BUCKET_NAME, storage.getBucketName());
      assertEquals(TEST_CONFIG_PATH_PREFIX, storage.getPathPrefix());
    }

    @Test
    @DisplayName("버킷 이름이 없으면 ConfigException이 발생한다")
    void shouldThrowConfigExceptionWhenBucketNameIsMissing() {
      // Given
      Map<String, String> configs = new HashMap<>();

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertTrue(
          exception.getMessage().contains("Missing required configuration \"storage.s3.bucket.name\""));
    }

    @Test
    @DisplayName("S3 클라이언트가 이미 주입된 경우 새로 생성하지 않는다")
    void shouldNotCreateNewClientIfInjected() throws NoSuchFieldException, IllegalAccessException {
      // Given
      S3Storage injectedStorage = new S3Storage(s3Client);
      Map<String, String> configs = createDefaultConfigs();

      // When
      injectedStorage.configure(configs);

      // Then
      Field clientField = S3Storage.class.getDeclaredField("s3Client");
      clientField.setAccessible(true);
      S3Client actualClient = (S3Client) clientField.get(injectedStorage);

      assertSame(s3Client, actualClient, "configure should not replace the injected S3 client");
    }
  }

  @Nested
  @DisplayName("store 메서드 테스트")
  class StoreTests {

    @BeforeEach
    void setUp() {
      // Inject mock S3 client for store tests
      storage = new S3Storage(s3Client);
      Map<String, String> configs = createDefaultConfigs();
      storage.configure(configs);
    }

    @Test
    @DisplayName("정상 데이터를 저장하면 S3 URI를 반환한다")
    void shouldReturnS3UriWhenStoringValidData() throws Exception {
      // Given
      byte[] payload = "test-payload".getBytes();
      when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenReturn(PutObjectResponse.builder().build());

      // When
      String result = storage.store(payload);

      // Then
      ArgumentCaptor<PutObjectRequest> requestCaptor =
          ArgumentCaptor.forClass(PutObjectRequest.class);
      ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

      verify(s3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());

      PutObjectRequest capturedRequest = requestCaptor.getValue();
      String generatedKey = capturedRequest.key();

      assertTrue(generatedKey.startsWith(TEST_CONFIG_PATH_PREFIX + "/"));
      String uuidPart = generatedKey.substring(TEST_CONFIG_PATH_PREFIX.length() + 1);
      assertDoesNotThrow(() -> UUID.fromString(uuidPart));
      assertEquals(TEST_CONFIG_BUCKET_NAME, capturedRequest.bucket());

      String expectedUri = "s3://" + TEST_CONFIG_BUCKET_NAME + "/" + generatedKey;
      assertEquals(expectedUri, result);

      assertArrayEquals(payload, readRequestBody(bodyCaptor.getValue()));
    }

    @Test
    @DisplayName("S3 업로드 실패 시 RuntimeException이 발생한다")
    void shouldThrowRuntimeExceptionOnS3UploadFailure() {
      // Given
      byte[] payload = "test-payload".getBytes();
      RuntimeException s3Exception = new RuntimeException("S3 connection failed");
      when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenThrow(s3Exception);

      // When
      RuntimeException exception = assertThrows(RuntimeException.class, () -> storage.store(payload));

      // Then
      assertTrue(exception.getMessage().contains("Failed to upload to S3"));
      assertEquals(s3Exception, exception.getCause());
      verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("configure를 호출하지 않으면 IllegalStateException이 발생한다")
    void shouldThrowIllegalStateExceptionIfNotConfigured() {
      // Given
      S3Storage unconfiguredStorage = new S3Storage(); // No client, not configured
      byte[] payload = "test-payload".getBytes();

      // When & Then
      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> unconfiguredStorage.store(payload));
      assertEquals("S3Client is not initialized. Call configure() first.", exception.getMessage());
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTests {

    @Test
    @DisplayName("S3Client가 주입된 상태에서 close 호출 시 client의 close가 호출된다")
    void shouldCloseInjectedClient() {
      // Given
      storage = new S3Storage(s3Client);
      doNothing().when(s3Client).close();

      // When
      storage.close();

      // Then
      verify(s3Client, times(1)).close();
    }

    @Test
    @DisplayName("S3Client가 null이어도 예외가 발생하지 않는다")
    void shouldNotThrowExceptionWhenClosingNullClient() {
      // Given
      storage = new S3Storage(); // No client injected or created

      // When & Then
      assertDoesNotThrow(() -> storage.close());
    }
  }

  private byte[] readRequestBody(RequestBody requestBody) throws Exception {
    try (InputStream inputStream = requestBody.contentStreamProvider().newStream()) {
      return inputStream.readAllBytes();
    }
  }
}
