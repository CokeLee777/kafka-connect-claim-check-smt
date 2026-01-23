package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
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

  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final String TEST_LARGE_PAYLOAD =
      "this is a large payload for integration testing";

  private static S3Client verificationS3Client;
  private S3Storage storage;
  private Map<String, String> configs;

  @BeforeAll
  static void beforeAll() {
    verificationS3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();
    verificationS3Client.createBucket(builder -> builder.bucket(TEST_BUCKET_NAME));
  }

  @AfterAll
  static void afterAll() {
    if (verificationS3Client != null) {
      verificationS3Client.close();
    }
  }

  @BeforeEach
  void setUp() {
    storage = new S3Storage();
    configs = new HashMap<>();
    configs.put(S3Storage.Config.BUCKET_NAME, TEST_BUCKET_NAME);
    configs.put(S3Storage.Config.REGION, localstack.getRegion());
    configs.put(
        S3Storage.Config.ENDPOINT_OVERRIDE,
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
  }

  @AfterEach
  void tearDown() {
    if (storage != null) {
      storage.close();
    }
  }

  @Test
  @DisplayName("configure와 store를 통해 S3에 객체를 정상적으로 업로드하고 referenceUrl을 반환한다")
  void shouldStoreObjectInS3AndReturnReferenceUrl() throws IOException {
    // Given
    storage.configure(configs);
    byte[] payload = TEST_LARGE_PAYLOAD.getBytes(StandardCharsets.UTF_8);

    // When
    String referenceUrl = storage.store(payload);

    // Then
    assertNotNull(referenceUrl);
    assertTrue(referenceUrl.startsWith("s3://" + TEST_BUCKET_NAME));

    String key = referenceUrl.substring(("s3://" + TEST_BUCKET_NAME + "/").length());
    byte[] retrievedPayload =
        verificationS3Client
            .getObject(GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(key).build())
            .readAllBytes();
    assertArrayEquals(payload, retrievedPayload);
  }

  @Test
  @DisplayName("네트워크 오류 발생 시 S3 업로드를 재시도하여 성공한다")
  void shouldRetryAndSucceedOnHttpFailure() throws IOException {
    // Given
    final int failureCount = 2;
    final FailingHttpClient failingHttpClient = new FailingHttpClient(failureCount);

    configs.put(
        S3Storage.Config.ENDPOINT_OVERRIDE,
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    configs.put(S3Storage.Config.RETRY_MAX, String.valueOf(failureCount));
    configs.put(S3Storage.Config.RETRY_BACKOFF_MS, "10");

    SimpleConfig simpleConfig = new SimpleConfig(S3Storage.Config.DEFINITION, configs);
    S3ClientConfig s3ClientConfig = S3ClientConfigFactory.create(simpleConfig);
    S3ClientFactory s3ClientFactory =
        new S3ClientFactory(
            new S3RetryStrategyFactory(),
            failingHttpClient,
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())));

    try (S3Client s3Client = s3ClientFactory.create(s3ClientConfig)) {

      storage = new S3Storage(s3Client);
      storage.configure(configs);
      byte[] data = TEST_LARGE_PAYLOAD.getBytes(StandardCharsets.UTF_8);

      // When
      String referenceUrl = storage.store(data);

      // Then
      assertNotNull(referenceUrl);
      assertEquals(failureCount + 1, failingHttpClient.callCount.get());

      String key = referenceUrl.substring(("s3://" + TEST_BUCKET_NAME + "/").length());
      byte[] retrievedPayload =
          verificationS3Client
              .getObject(GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(key).build())
              .readAllBytes();
      assertArrayEquals(data, retrievedPayload);
    }
  }

  @Test
  @DisplayName("close 메서드가 S3 클라이언트를 정상적으로 닫는다")
  void shouldCloseS3Client() {
    // Given
    storage.configure(configs);

    // When & Then
    assertDoesNotThrow(() -> storage.close());
  }

  // Helper class for simulating HTTP failures
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
            throw new IOException("Simulated network error on attempt " + callCount.get());
          }
          return delegate.prepareRequest(request).call();
        }

        @Override
        public void abort() {
          delegate.prepareRequest(request).abort();
        }
      };
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
