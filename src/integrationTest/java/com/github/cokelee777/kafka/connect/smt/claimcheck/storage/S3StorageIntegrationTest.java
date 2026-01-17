package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Testcontainers
@DisplayName("S3Storage 통합 테스트")
class S3StorageIntegrationTest {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.2.0");
  private static final String BUCKET_NAME = "test-bucket";

  @Container
  private static final LocalStackContainer localstack =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.S3);

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
    s3Client.createBucket(builder -> builder.bucket(BUCKET_NAME));
  }

  @AfterAll
  static void afterAll() {
    s3Client.close();
  }

  private Map<String, String> createS3Config() {
    Map<String, String> config = new HashMap<>();
    config.put(S3Storage.CONFIG_BUCKET_NAME, BUCKET_NAME);
    config.put(S3Storage.CONFIG_REGION, localstack.getRegion());
    config.put(
        S3Storage.CONFIG_ENDPOINT_OVERRIDE,
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    config.put("storage.s3.credentials.access.key.id", localstack.getAccessKey());
    config.put("storage.s3.credentials.secret.access.key", localstack.getSecretKey());
    return config;
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
  @DisplayName("configure와 store를 통해 S3에 객체를 정상적으로 업로드하고 URI를 반환한다")
  void shouldStoreObjectInS3AndReturnURI() throws IOException {
    // Given
    storage = new S3Storage();
    Map<String, String> config = createS3Config();
    storage.configure(config);
    byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);

    // When
    String s3uri = storage.store(data);

    // Then
    assertNotNull(s3uri);
    assertTrue(s3uri.startsWith("s3://" + BUCKET_NAME + "/"));

    String key = s3uri.substring(("s3://" + BUCKET_NAME + "/").length());
    byte[] storedData =
        s3Client
            .getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build())
            .readAllBytes();
    assertArrayEquals(data, storedData);
  }

  @Test
  @DisplayName("네트워크 오류로 S3 업로드 실패 시, 재시도하여 성공한다")
  void shouldRetryAndSucceedOnHttpFailure() throws IOException {
    // Given
    try (FailingHttpClient failingHttpClient = new FailingHttpClient(1)) {
      Map<String, String> config = createS3Config();
      config.put(S3Storage.CONFIG_RETRY_MAX, "1");
      config.put(S3Storage.CONFIG_RETRY_BACKOFF_MS, "100");

      StandardRetryStrategy retryStrategy =
          StandardRetryStrategy.builder()
              .maxAttempts(Integer.parseInt(config.get(S3Storage.CONFIG_RETRY_MAX)) + 1)
              .backoffStrategy(
                  BackoffStrategy.exponentialDelay(
                      Duration.ofMillis(
                          Long.parseLong(config.get(S3Storage.CONFIG_RETRY_BACKOFF_MS))),
                      Duration.ofSeconds(1)))
              .build();

      try (S3Client customS3Client =
          S3Client.builder()
              .httpClient(failingHttpClient)
              .endpointOverride(URI.create(config.get(S3Storage.CONFIG_ENDPOINT_OVERRIDE)))
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create(
                          config.get("storage.s3.credentials.access.key.id"),
                          config.get("storage.s3.credentials.secret.access.key"))))
              .region(Region.of(config.get(S3Storage.CONFIG_REGION)))
              .overrideConfiguration(
                  ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build())
              .forcePathStyle(true)
              .build()) {
        // Given
        storage = new S3Storage(customS3Client);
        storage.configure(config);
        byte[] data = "hello retry".getBytes(StandardCharsets.UTF_8);

        // When
        String s3uri = storage.store(data);

        // Then
        assertNotNull(s3uri);
        assertTrue(s3uri.startsWith("s3://" + BUCKET_NAME + "/"));
        // SDK 내부 요청(메타데이터 조회, 프리플라이트 검사 등)이 발생하면 호출 횟수가 2를 초과할 수 있으므로 2 이상인지 확인
        assertTrue(failingHttpClient.callCount.get() >= 2);

        // Then
        String key = s3uri.substring(("s3://" + BUCKET_NAME + "/").length());
        byte[] storedData =
            s3Client
                .getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build())
                .readAllBytes();
        assertArrayEquals(data, storedData);
      }
    }
  }

  @Test
  @DisplayName("close 메서드가 S3 클라이언트를 정상적으로 닫는다")
  void shouldCloseS3Client() {
    // Given
    storage = new S3Storage();
    storage.configure(createS3Config());

    // When & Then
    assertDoesNotThrow(() -> storage.close());
  }
}
