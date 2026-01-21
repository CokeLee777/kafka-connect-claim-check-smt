package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryStrategyFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3RetryConfigAdapter;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Testcontainers
@DisplayName("S3Storage 통합 테스트")
class S3StorageIntegrationTest {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.2.0");

  @Container
  private static final LocalStackContainer localstack =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.S3);

  private static final String TEST_CONFIG_BUCKET_NAME = "test-bucket";
  private static final String TEST_CONFIG_RETRY_MAX = "5";
  private static final String TEST_CONFIG_RETRY_BACKOFF_MS = "100";
  private static final String TEST_CONFIG_RETRY_MAX_BACKOFF_MS = "1000";
  private static final String TEST_LARGE_PAYLOAD = "this is large payload this is large payload!!!";
  private static final int TEST_RETRY_MAX = 5;
  private static final long TEST_RETRY_BACKOFF_MS = 100L;
  private static final long TEST_RETRY_MAX_BACKOFF_MS = 1000L;

  private static S3Client s3Client;
  private S3Storage storage;

  @BeforeAll
  static void beforeAll() {
    s3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();
    s3Client.createBucket(builder -> builder.bucket(TEST_CONFIG_BUCKET_NAME));
  }

  @AfterAll
  static void afterAll() {
    s3Client.close();
  }

  @BeforeEach
  void setUp() {
    storage = new S3Storage();
  }

  private Map<String, String> createS3StorageConfig() {
    return Map.of(
        S3Storage.Config.BUCKET_NAME,
        TEST_CONFIG_BUCKET_NAME,
        S3Storage.Config.REGION,
        localstack.getRegion(),
        S3Storage.Config.ENDPOINT_OVERRIDE,
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
        S3Storage.Config.RETRY_MAX,
        TEST_CONFIG_RETRY_MAX,
        S3Storage.Config.RETRY_BACKOFF_MS,
        TEST_CONFIG_RETRY_BACKOFF_MS,
        S3Storage.Config.RETRY_MAX_BACKOFF_MS,
        TEST_CONFIG_RETRY_MAX_BACKOFF_MS);
  }

  private static class FailingHttpClient implements SdkHttpClient {
    private final SdkHttpClient delegate = UrlConnectionHttpClient.create();
    private final AtomicInteger callCount = new AtomicInteger(0);
    private final int failureCount;

    FailingHttpClient(int failureCount) {
      this.failureCount = failureCount;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (callCount.getAndIncrement() < failureCount) {
            throw new IOException("Simulated network error");
          }
          return delegate.prepareRequest(request).call();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  @Test
  @DisplayName("configure와 store를 통해 S3에 객체를 정상적으로 업로드하고 referenceUrl을 반환한다")
  void shouldStoreObjectInS3AndReturnReferenceUrl() throws IOException {
    // Given
    storage.configure(createS3StorageConfig());
    byte[] payload = TEST_LARGE_PAYLOAD.getBytes(StandardCharsets.UTF_8);

    // When
    String referenceUrl = storage.store(payload);

    // Then
    assertNotNull(referenceUrl);
    assertTrue(referenceUrl.startsWith("s3://" + TEST_CONFIG_BUCKET_NAME));

    String key = referenceUrl.substring(("s3://" + TEST_CONFIG_BUCKET_NAME + "/").length());
    byte[] retrievedPayload =
        s3Client
            .getObject(GetObjectRequest.builder().bucket(TEST_CONFIG_BUCKET_NAME).key(key).build())
            .readAllBytes();
    assertArrayEquals(payload, retrievedPayload);
  }

  @Test
  @DisplayName("네트워크 오류로 S3 업로드 실패 시, 재시도하여 성공한다")
  void shouldRetryAndSucceedOnHttpFailure() throws IOException {
    // Given
    try (FailingHttpClient failingHttpClient = new FailingHttpClient(1)) {
      Map<String, String> configs = new HashMap<>(createS3StorageConfig());

      RetryConfig retryConfig =
          new RetryConfig(
              TEST_RETRY_MAX + 1,
              Duration.ofMillis(TEST_RETRY_BACKOFF_MS),
              Duration.ofMillis(TEST_RETRY_MAX_BACKOFF_MS));

      RetryStrategyFactory<StandardRetryStrategy> retryStrategyFactory = new S3RetryConfigAdapter();
      StandardRetryStrategy retryStrategy = retryStrategyFactory.create(retryConfig);

      try (S3Client customS3Client =
          S3Client.builder()
              .httpClient(failingHttpClient)
              .endpointOverride(URI.create(configs.get(S3Storage.Config.ENDPOINT_OVERRIDE)))
              .credentialsProvider(DefaultCredentialsProvider.builder().build())
              .region(Region.of(configs.get(S3Storage.Config.REGION)))
              .overrideConfiguration(
                  ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build())
              .forcePathStyle(true)
              .build()) {

        storage = new S3Storage(customS3Client, retryStrategyFactory);
        storage.configure(configs);
        byte[] data = TEST_LARGE_PAYLOAD.getBytes(StandardCharsets.UTF_8);

        // When
        String referenceUrl = storage.store(data);

        // Then
        assertNotNull(referenceUrl);
        assertTrue(referenceUrl.startsWith("s3://" + TEST_CONFIG_BUCKET_NAME));

        // SDK 내부 요청(메타데이터 조회, 프리플라이트 검사 등)이 발생하면 호출 횟수가 2를 초과할 수 있으므로 2 이상인지 확인
        assertTrue(failingHttpClient.callCount.get() >= 2);

        String key = referenceUrl.substring(("s3://" + TEST_CONFIG_BUCKET_NAME + "/").length());
        byte[] retrievedPayload =
            s3Client
                .getObject(
                    GetObjectRequest.builder().bucket(TEST_CONFIG_BUCKET_NAME).key(key).build())
                .readAllBytes();
        assertArrayEquals(data, retrievedPayload);
      }
    }
  }

  @Test
  @DisplayName("close 메서드가 S3 클라이언트를 정상적으로 닫는다")
  void shouldCloseS3Client() {
    // Given
    storage.configure(createS3StorageConfig());

    // When & Then
    assertDoesNotThrow(() -> storage.close());
  }
}
