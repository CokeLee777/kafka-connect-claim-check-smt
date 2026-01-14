package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
// S3ClientBuilder import 추가
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3Storage 단위 테스트")
class S3StorageTest {

  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final String TEST_REGION_AP_NORTHEAST_1 = "ap-northeast-1";
  private static final String TEST_REGION_AP_NORTHEAST_2 = "ap-northeast-2";
  private static final String TEST_PATH_PREFIX = "my-prefix";
  private static final String TEST_ENDPOINT_LOCALSTACK = "http://localhost:4566";
  private static final String EXPECTED_MISSING_BUCKET_ERROR_MESSAGE =
      "Missing required configuration \"storage.s3.bucket.name\" which has no default value.";
  private static final String EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE =
      "Configuration \"storage.s3.bucket.name\" must not be empty or blank.";
  private static final String EXPECTED_EMPTY_REGION_ERROR_MESSAGE =
      "region must not be blank or empty.";
  private static final String EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE =
      "Configuration \"storage.s3.endpoint.override\" must not be empty or blank if provided.";

  private S3Storage storage;
  @Mock private S3Client s3Client;

  @BeforeEach
  void setUp() {
    storage = new S3Storage();
  }

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureMethodTests {

    @Nested
    @DisplayName("성공 케이스")
    class ConfigureSuccessCases {

      @Test
      @DisplayName("필수 설정(버킷)만 제공하면 기본값(리전, prefix)으로 정상 초기화된다")
      void configureWithRequiredFieldsOnly() {
        // Given
        Map<String, String> configs = createConfigWithBucket(TEST_BUCKET_NAME);

        // When
        storage.configure(configs);

        // Then
        assertAll(
            () -> assertEquals(TEST_BUCKET_NAME, storage.getBucketName()),
            () -> assertEquals(TEST_REGION_AP_NORTHEAST_2, storage.getRegion()),
            () -> assertEquals("claim-checks", storage.getPathPrefix()),
            () -> assertNull(storage.getEndpointOverride()));
      }

      @Test
      @DisplayName("모든 설정값이 제공되면 정상적으로 초기화된다")
      void configureWithAllFields() {
        // Given
        Map<String, String> configs =
            createConfigWithAllFields(
                TEST_BUCKET_NAME,
                TEST_REGION_AP_NORTHEAST_1,
                TEST_ENDPOINT_LOCALSTACK,
                TEST_PATH_PREFIX);

        // When
        storage.configure(configs);

        // Then
        assertAll(
            () -> assertEquals(TEST_BUCKET_NAME, storage.getBucketName()),
            () -> assertEquals(TEST_REGION_AP_NORTHEAST_1, storage.getRegion()),
            () -> assertEquals(TEST_ENDPOINT_LOCALSTACK, storage.getEndpointOverride()),
            () -> assertEquals(TEST_PATH_PREFIX, storage.getPathPrefix()));
      }
    }

    @Nested
    @DisplayName("실패 케이스")
    class ConfigureFailureCases {

      @Test
      @DisplayName("버킷 이름이 없으면 ConfigException이 발생한다")
      void configureWithoutBucketThrowsException() {
        // Given
        Map<String, String> configs = new HashMap<>();
        configs.put(S3Storage.CONFIG_REGION, TEST_REGION_AP_NORTHEAST_1);

        // When
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));

        // Then
        assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("빈 문자열 버킷 이름이면 ConfigException이 발생한다")
      void configureWithEmptyBucketName() {
        // Given
        Map<String, String> configs = createConfigWithBucket("");

        // When
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));

        // Then
        assertEquals(EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("빈 문자열 엔드포인트면 ConfigException이 발생한다")
      void configureWithEmptyEndpoint() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "");

        // When
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));

        // Then
        assertEquals(EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("공백으로만 된 엔드포인트면 ConfigException이 발생한다")
      void configureWithBlankEndpoint() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "   ");

        // When
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));

        // Then
        assertEquals(EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE, exception.getMessage());
      }
    }

    @Nested
    @DisplayName("경계값 테스트")
    class ConfigureEdgeCases {

      @Test
      @DisplayName("공백이 포함된 버킷 이름도 정상적으로 처리된다")
      void configureWithWhitespaceBucketName() {
        // Given
        Map<String, String> configs = createConfigWithBucket("  test-bucket  ");

        // When
        storage.configure(configs);

        // Then
        // ConfigUtils.getRequiredString이 trim을 하므로 공백이 제거된 이름이 설정되어야 함
        assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      }

      @Test
      @DisplayName("빈 문자열 리전이면 IllegalArgumentException이 발생한다")
      void configureWithEmptyRegion() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, "");

        // When
        IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> storage.configure(configs));

        // Then
        assertEquals(EXPECTED_EMPTY_REGION_ERROR_MESSAGE, exception.getMessage());
      }
    }

    @Nested
    @DisplayName("pathPrefix 정규화 테스트")
    class PathPrefixNormalizationTests {

      private void assertPathPrefix(String input, String expected) {
        Map<String, String> configs = createConfigWithBucket(TEST_BUCKET_NAME);
        configs.put(S3Storage.CONFIG_S3_PATH_PREFIX, input);
        storage.configure(configs);
        assertEquals(expected, storage.getPathPrefix());
      }

      @Test
      @DisplayName("'/'로 끝나지 않는 pathPrefix는 그대로 유지된다")
      void shouldAddTrailingSlash() {
        assertPathPrefix("my-prefix", "my-prefix");
      }

      @Test
      @DisplayName("'/'로 끝나는 pathPrefix는 '/'가 제거된다")
      void shouldKeepSingleTrailingSlash() {
        assertPathPrefix("my-prefix/", "my-prefix");
      }

      @Test
      @DisplayName("여러 개의 '/'로 끝나는 pathPrefix는 '/'가 모두 제거된다")
      void shouldNormalizeMultipleSlashes() {
        assertPathPrefix("my-prefix//", "my-prefix");
      }

      @Test
      @DisplayName("여러 개의 '/'를 가지는 pathPrefix는 마지막 '/'들만 제거된다")
      void shouldNormalizeMultipleTrailingSlashes() {
        assertPathPrefix("my-prefix/1/2/3//", "my-prefix/1/2/3");
      }

      @Test
      @DisplayName("공백을 포함하는 pathPrefix는 trim된다")
      void shouldTrimAndAddSlash() {
        assertPathPrefix("  my-prefix  ", "my-prefix");
      }

      @Test
      @DisplayName("빈 pathPrefix는 그대로 유지된다")
      void shouldNormalizeEmptyPrefix() {
        assertPathPrefix("", "");
      }

      @Test
      @DisplayName("공백으로만 된 pathPrefix는 빈 pathPrefix로 된다")
      void shouldNormalizeBlankPrefix() {
        assertPathPrefix("   ", "");
      }

      @Test
      @DisplayName("'/'만 있는 pathPrefix는 '/'가 제거된다")
      void shouldKeepSlashOnlyPrefix() {
        assertPathPrefix("/", "");
      }
    }
  }

  private Map<String, String> createConfigWithBucket(String bucket) {
    Map<String, String> configs = new HashMap<>();
    configs.put(S3Storage.CONFIG_BUCKET_NAME, bucket);
    return configs;
  }

  private Map<String, String> createConfigWithBucketAndRegion(String bucket, String region) {
    Map<String, String> configs = createConfigWithBucket(bucket);
    configs.put(S3Storage.CONFIG_REGION, region);
    return configs;
  }

  private Map<String, String> createConfigWithBucketAndEndpoint(String bucket, String endpoint) {
    Map<String, String> configs = createConfigWithBucket(bucket);
    configs.put(S3Storage.CONFIG_ENDPOINT_OVERRIDE, endpoint);
    return configs;
  }

  private Map<String, String> createConfigWithAllFields(
      String bucket, String region, String endpoint, String prefix) {
    Map<String, String> configs = createConfigWithBucketAndRegion(bucket, region);
    configs.put(S3Storage.CONFIG_ENDPOINT_OVERRIDE, endpoint);
    configs.put(S3Storage.CONFIG_S3_PATH_PREFIX, prefix);
    return configs;
  }

  @Nested
  @DisplayName("store 메서드 테스트")
  class StoreMethodTests {
    @BeforeEach
    void setup() {
      // Given (setup for store tests)
      Map<String, String> configs = new HashMap<>();
      configs.put(S3Storage.CONFIG_BUCKET_NAME, TEST_BUCKET_NAME);
      configs.put(S3Storage.CONFIG_S3_PATH_PREFIX, TEST_PATH_PREFIX);
      storage.configure(configs);
      try {
        Field clientField = S3Storage.class.getDeclaredField("s3Client");
        clientField.setAccessible(true);
        clientField.set(storage, s3Client);
      } catch (Exception e) {
        fail("Test setup failed for S3Client injection");
      }
    }

    @Nested
    @DisplayName("성공 케이스")
    class StoreSuccessCases {
      @Test
      @DisplayName("정상적인 데이터를 저장하면 S3 URI를 반환한다")
      void storeWithValidDataReturnsS3Uri() throws Exception {
        // Given
        byte[] data = "test-data".getBytes();
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When
        String result = storage.store(data);

        // Then
        ArgumentCaptor<PutObjectRequest> requestCaptor =
            ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
        PutObjectRequest capturedRequest = requestCaptor.getValue();
        String generatedKey = capturedRequest.key();

        assertTrue(generatedKey.startsWith(TEST_PATH_PREFIX));
        String uuidPart = generatedKey.substring(TEST_PATH_PREFIX.length() + 1);
        assertDoesNotThrow(() -> UUID.fromString(uuidPart));
        assertEquals(TEST_BUCKET_NAME, capturedRequest.bucket());
        String expectedUri = "s3://" + TEST_BUCKET_NAME + "/" + generatedKey;
        assertEquals(expectedUri, result);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(any(PutObjectRequest.class), bodyCaptor.capture());
        assertArrayEquals(data, readRequestBody(bodyCaptor.getValue()));
      }

      @Test
      @DisplayName("빈 바이트 배열도 정상적으로 저장된다")
      void storeWithEmptyData() throws Exception {
        // Given
        byte[] data = new byte[0];
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When
        String result = storage.store(data);

        // Then
        ArgumentCaptor<PutObjectRequest> requestCaptor =
            ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
        String generatedKey = requestCaptor.getValue().key();
        String expectedUri = "s3://" + TEST_BUCKET_NAME + "/" + generatedKey;
        assertEquals(expectedUri, result);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(any(PutObjectRequest.class), bodyCaptor.capture());
        assertArrayEquals(data, readRequestBody(bodyCaptor.getValue()));
      }
    }

    @Nested
    @DisplayName("실패 케이스")
    class StoreFailureCases {
      @Test
      @DisplayName("configure를 호출하지 않으면 IllegalStateException이 발생한다")
      void storeWithoutConfigureThrowsException() {
        // Given
        S3Storage unconfiguredStorage = new S3Storage();
        byte[] data = "test-data".getBytes();

        // When & Then
        IllegalStateException exception =
            assertThrows(IllegalStateException.class, () -> unconfiguredStorage.store(data));
        assertEquals(
            "S3Client is not initialized. Call configure() first.", exception.getMessage());
      }

      @Test
      @DisplayName("S3 업로드 실패 시 RuntimeException이 발생한다")
      void storeWhenS3UploadFailsThrowsException() {
        // Given
        byte[] data = "test-data".getBytes();
        RuntimeException s3Exception = new RuntimeException("S3 connection failed");
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenThrow(s3Exception);

        // When
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> storage.store(data));

        // Then
        ArgumentCaptor<PutObjectRequest> requestCaptor =
            ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
        String generatedKey = requestCaptor.getValue().key();

        assertTrue(exception.getMessage().contains("Failed to upload to S3"));
        assertTrue(exception.getMessage().contains(TEST_BUCKET_NAME));
        assertTrue(exception.getMessage().contains(generatedKey));
        assertEquals(s3Exception, exception.getCause());
      }

      @Test
      @DisplayName("null data로 저장하면 RuntimeException이 발생한다")
      void storeWithNullDataThrowsException() {
        // Given
        byte[] data = null;

        // When
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> storage.store(data));

        // Then
        assertTrue(exception.getCause() instanceof NullPointerException);
      }
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseMethodTests {
    @Test
    @DisplayName("S3Client가 설정된 상태에서 close를 호출하면 정상적으로 닫힌다")
    void closeWithS3ClientClosesS3Client() {
      // Given
      storage.configure(createConfigWithBucket(TEST_BUCKET_NAME));
      try {
        Field clientField = S3Storage.class.getDeclaredField("s3Client");
        clientField.setAccessible(true);
        clientField.set(storage, s3Client);
      } catch (Exception e) {
        fail("Test setup failed for S3Client injection");
      }
      doNothing().when(s3Client).close();

      // When
      storage.close();

      // Then
      verify(s3Client, times(1)).close();
    }

    @Test
    @DisplayName("S3Client가 null이어도 예외가 발생하지 않는다")
    void closeWhenS3ClientIsNullDoesNotThrowException() {
      // Given
      S3Storage unconfiguredStorage = new S3Storage();

      // When & Then
      assertDoesNotThrow(unconfiguredStorage::close);
    }
  }

  private byte[] readRequestBody(RequestBody requestBody) throws Exception {
    try (InputStream inputStream = requestBody.contentStreamProvider().newStream()) {
      return inputStream.readAllBytes();
    }
  }
}
