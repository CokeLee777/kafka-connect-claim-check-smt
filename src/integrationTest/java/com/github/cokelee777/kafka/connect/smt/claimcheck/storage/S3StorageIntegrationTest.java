package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Testcontainers
@DisplayName("S3Storage 통합 테스트")
class S3StorageIntegrationTest {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.2.0");
  private static final String BUCKET_NAME = "test-bucket-" + UUID.randomUUID();

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

  @BeforeEach
  void setUp() {
    storage = new S3Storage();
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

  @Test
  @DisplayName("configure와 store를 통해 S3에 객체를 정상적으로 업로드하고 URI를 반환한다")
  void shouldStoreObjectInS3AndReturnURI() throws IOException {
    // Given
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
  @DisplayName("close 메서드가 S3 클라이언트를 정상적으로 닫는다")
  void shouldCloseS3Client() {
    // Given
    storage.configure(createS3Config());

    // When & Then
    assertDoesNotThrow(() -> storage.close());
  }
}
