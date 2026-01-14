package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
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

  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final String TEST_REGION_AP_NORTHEAST_1 = "ap-northeast-1";
  private static final String TEST_REGION_AP_NORTHEAST_2 = "ap-northeast-2";
  private static final String TEST_ENDPOINT_LOCALSTACK = "http://localhost:4566";
  private static final String EXPECTED_MISSING_BUCKET_ERROR_MESSAGE =
      "Missing required configuration \"storage.s3.bucket.name\" which has no default value.";
  private static final String EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE =
      "Configuration \"storage.s3.bucket.name\" must not be empty or blank.";
  private static final String EXPECTED_EMPTY_REGION_ERROR_MESSAGE =
      "Configuration \"storage.s3.region\" must not be empty or blank if provided.";
  private static final String EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE =
      "Configuration \"storage.s3.endpoint.override\" must not be empty or blank if provided.";

  @Mock private S3Client s3Client;

  private S3Storage storage;

  @BeforeEach
  void setUp() {
    storage = new S3Storage(s3Client);
  }

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureMethodTests {

    @Nested
    @DisplayName("성공 케이스")
    class ConfigureSuccessCases {

      @Test
      @DisplayName("필수 설정(버킷, 리전)만 제공하면 정상적으로 초기화된다")
      void configureWithRequiredFieldsOnly() {
        // Given
        Map<String, String> configs =
            createConfigWithBucketAndRegion(TEST_BUCKET_NAME, TEST_REGION_AP_NORTHEAST_1);

        // When
        storage.configure(configs);

        // Then
        assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
        assertEquals(TEST_REGION_AP_NORTHEAST_1, storage.getRegion());
        assertNull(storage.getEndpointOverride());
      }

      @Test
      @DisplayName("버킷만 제공하면 기본 리전(ap-northeast-2)으로 초기화된다")
      void configureWithBucketOnlyUsesDefaultRegion() {
        // Given
        Map<String, String> configs = createConfigWithBucket(TEST_BUCKET_NAME);

        // When
        storage.configure(configs);

        // Then
        assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
        assertEquals(TEST_REGION_AP_NORTHEAST_2, storage.getRegion());
        assertNull(storage.getEndpointOverride());
      }

      @Test
      @DisplayName("엔드포인트 오버라이드 설정이 정상적으로 파싱된다")
      void configureWithEndpointOverride() {
        // Given
        Map<String, String> configs =
            createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, TEST_ENDPOINT_LOCALSTACK);

        // When
        storage.configure(configs);

        // Then
        assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
        assertEquals(TEST_ENDPOINT_LOCALSTACK, storage.getEndpointOverride());
      }

      @Test
      @DisplayName("모든 설정값이 제공되면 정상적으로 초기화된다")
      void configureWithAllFields() {
        // Given
        Map<String, String> configs =
            createConfigWithAllFields(
                TEST_BUCKET_NAME, TEST_REGION_AP_NORTHEAST_1, TEST_ENDPOINT_LOCALSTACK);

        // When
        storage.configure(configs);

        // Then
        assertAll(
            () -> assertEquals(TEST_BUCKET_NAME, storage.getBucketName()),
            () -> assertEquals(TEST_REGION_AP_NORTHEAST_1, storage.getRegion()),
            () -> assertEquals(TEST_ENDPOINT_LOCALSTACK, storage.getEndpointOverride()));
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
        configs.put("storage.s3.region", TEST_REGION_AP_NORTHEAST_1);

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("빈 설정 맵으로 초기화하면 ConfigException이 발생한다")
      void configureWithEmptyMapThrowsException() {
        // Given
        Map<String, String> configs = new HashMap<>();

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("빈 문자열 버킷 이름이면 ConfigException이 발생한다")
      void configureWithEmptyBucketName() {
        // Given
        Map<String, String> configs = createConfigWithBucket("");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("공백으로만 된 버킷 이름이면 ConfigException이 발생한다")
      void configureWithBlankBucketName() {
        // Given
        Map<String, String> configs = createConfigWithBucket("   ");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("빈 문자열 엔드포인트면 ConfigException이 발생한다")
      void configureWithEmptyEndpoint() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("공백으로만 된 엔드포인트면 ConfigException이 발생한다")
      void configureWithBlankEndpoint() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "   ");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
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
        String bucketWithWhitespace = "  test-bucket  ";
        Map<String, String> configs = createConfigWithBucket(bucketWithWhitespace);

        // When
        storage.configure(configs);

        // Then
        assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      }

      @Test
      @DisplayName("빈 문자열 리전이면 ConfigException이 발생한다")
      void configureWithEmptyRegion() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, "");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_EMPTY_REGION_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("공백으로만 된 리전이면 ConfigException이 발생한다")
      void configureWithBlankRegion() {
        // Given
        Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, "   ");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> storage.configure(configs));
        assertEquals(EXPECTED_EMPTY_REGION_ERROR_MESSAGE, exception.getMessage());
      }

      @Test
      @DisplayName("잘못된 URI 형식의 엔드포인트면 예외가 발생한다")
      void configureWithInvalidEndpointUri() {
        // Given
        Map<String, String> configs =
            createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "not-a-valid-uri://");

        // When & Then
        assertThrows(Exception.class, () -> storage.configure(configs));
      }

    }
  }

  // 테스트 데이터 생성 헬퍼 메서드
  private Map<String, String> createConfigWithBucket(String bucket) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    return configs;
  }

  private Map<String, String> createConfigWithBucketAndRegion(String bucket, String region) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.region", region);
    return configs;
  }

  private Map<String, String> createConfigWithBucketAndEndpoint(String bucket, String endpoint) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.endpoint.override", endpoint);
    return configs;
  }

  private Map<String, String> createConfigWithAllFields(
      String bucket, String region, String endpoint) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.region", region);
    configs.put("storage.s3.endpoint.override", endpoint);
    return configs;
  }

  @Nested
  @DisplayName("store 메서드 테스트")
  class StoreMethodTests {

    @Nested
    @DisplayName("성공 케이스")
    class StoreSuccessCases {

      @Test
      @DisplayName("정상적인 데이터를 저장하면 S3 URI를 반환한다")
      void storeWithValidDataReturnsS3Uri() throws Exception {
        // Given
        String key = "test-key";
        byte[] data = "test-data".getBytes();
        setBucketName(storage, TEST_BUCKET_NAME);

        PutObjectResponse response = PutObjectResponse.builder().build();
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(response);

        // When
        String result = storage.store(key, data);

        // Then
        String expectedUri = "s3://" + TEST_BUCKET_NAME + "/" + key;
        assertEquals(expectedUri, result);

        ArgumentCaptor<PutObjectRequest> requestCaptor =
            ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(1)).putObject(requestCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest capturedRequest = requestCaptor.getValue();
        assertEquals(TEST_BUCKET_NAME, capturedRequest.bucket());
        assertEquals(key, capturedRequest.key());

        RequestBody capturedBody = bodyCaptor.getValue();
        byte[] capturedData = readRequestBody(capturedBody);
        assertArrayEquals(data, capturedData);
      }

      @Test
      @DisplayName("빈 바이트 배열도 정상적으로 저장된다")
      void storeWithEmptyData() throws Exception {
        // Given
        String key = "empty-key";
        byte[] data = new byte[0];
        setBucketName(storage, TEST_BUCKET_NAME);

        PutObjectResponse response = PutObjectResponse.builder().build();
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(response);

        // When
        String result = storage.store(key, data);

        // Then
        String expectedUri = "s3://" + TEST_BUCKET_NAME + "/" + key;
        assertEquals(expectedUri, result);

        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), bodyCaptor.capture());

        RequestBody capturedBody = bodyCaptor.getValue();
        byte[] capturedData = readRequestBody(capturedBody);
        assertArrayEquals(data, capturedData);
      }

      @Test
      @DisplayName("긴 키 이름도 정상적으로 처리된다")
      void storeWithLongKey() throws Exception {
        // Given
        String key = "very/long/path/to/test-key-12345";
        byte[] data = "test-data".getBytes();
        setBucketName(storage, TEST_BUCKET_NAME);

        PutObjectResponse response = PutObjectResponse.builder().build();
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(response);

        // When
        String result = storage.store(key, data);

        // Then
        String expectedUri = "s3://" + TEST_BUCKET_NAME + "/" + key;
        assertEquals(expectedUri, result);

        ArgumentCaptor<PutObjectRequest> requestCaptor =
            ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, times(1)).putObject(requestCaptor.capture(), bodyCaptor.capture());

        assertEquals(key, requestCaptor.getValue().key());

        RequestBody capturedBody = bodyCaptor.getValue();
        byte[] capturedData = readRequestBody(capturedBody);
        assertArrayEquals(data, capturedData);
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
        String key = "test-key";
        byte[] data = "test-data".getBytes();

        // When & Then
        IllegalStateException exception =
            assertThrows(
                IllegalStateException.class, () -> unconfiguredStorage.store(key, data));
        assertEquals(
            "S3Client is not initialized. Call configure() first.", exception.getMessage());
      }

      @Test
      @DisplayName("S3 업로드 실패 시 RuntimeException이 발생한다")
      void storeWhenS3UploadFailsThrowsException() throws Exception {
        // Given
        String key = "test-key";
        byte[] data = "test-data".getBytes();
        setBucketName(storage, TEST_BUCKET_NAME);

        RuntimeException s3Exception = new RuntimeException("S3 connection failed");
        doThrow(s3Exception)
            .when(s3Client)
            .putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // When & Then
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> storage.store(key, data));
        assertTrue(exception.getMessage().contains("Failed to upload to S3"));
        assertTrue(exception.getMessage().contains(TEST_BUCKET_NAME));
        assertTrue(exception.getMessage().contains(key));
        assertEquals(s3Exception, exception.getCause());
      }

      @Test
      @DisplayName("null data로 저장하면 RuntimeException이 발생한다")
      void storeWithNullDataThrowsException() throws Exception {
        // Given
        String key = "test-key";
        byte[] data = null;
        setBucketName(storage, TEST_BUCKET_NAME);

        // When & Then
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> storage.store(key, data));
        assertTrue(exception.getMessage().contains("Failed to upload to S3"));
      }
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseMethodTests {

    @Nested
    @DisplayName("성공 케이스")
    class CloseSuccessCases {

      @Test
      @DisplayName("S3Client가 설정된 상태에서 close를 호출하면 정상적으로 닫힌다")
      void closeWithS3ClientClosesS3Client() {
        // Given
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
        assertDoesNotThrow(() -> unconfiguredStorage.close());
      }

      @Test
      @DisplayName("여러 번 close를 호출해도 안전하다")
      void closeMultipleTimesIsSafe() {
        // Given
        doNothing().when(s3Client).close();

        // When
        storage.close();
        storage.close();
        storage.close();

        // Then
        verify(s3Client, times(3)).close();
      }
    }

    @Nested
    @DisplayName("실패 케이스")
    class CloseFailureCases {

      @Test
      @DisplayName("S3Client close 중 예외가 발생해도 처리된다")
      void closeWhenS3ClientThrowsException() {
        // Given
        RuntimeException closeException = new RuntimeException("Close failed");
        doThrow(closeException).when(s3Client).close();

        // When & Then
        assertThrows(RuntimeException.class, () -> storage.close());
        verify(s3Client, times(1)).close();
      }
    }
  }

  // 테스트 헬퍼 메서드: reflection을 사용하여 bucketName 설정
  private void setBucketName(S3Storage storage, String bucketName) throws Exception {
    Field bucketNameField = S3Storage.class.getDeclaredField("bucketName");
    bucketNameField.setAccessible(true);
    bucketNameField.set(storage, bucketName);
  }

  // 테스트 헬퍼 메서드: RequestBody에서 데이터 읽기
  private byte[] readRequestBody(RequestBody requestBody) throws Exception {
    try (InputStream inputStream = requestBody.contentStreamProvider().newStream()) {
      return inputStream.readAllBytes();
    }
  }
}
