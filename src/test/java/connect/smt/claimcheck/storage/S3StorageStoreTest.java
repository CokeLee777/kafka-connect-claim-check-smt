package connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
public class S3StorageStoreTest {

  @Container
  private static final LocalStackContainer localStack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack"))
          .withServices(LocalStackContainer.Service.S3);

  private S3Client s3Client;

  @BeforeEach
  void setUp() {
    this.s3Client =
        S3Client.builder()
            .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localStack.getAccessKey(), localStack.getSecretKey())))
            .region(Region.of(localStack.getRegion()))
            .build();

    // 테스트용 버킷 생성
    this.s3Client.createBucket(b -> b.bucket("test-bucket"));
  }

  @Test
  @DisplayName("데이터를 업로드하면 S3 URL이 반환되고 실제 버킷에 저장되어야 한다")
  void storeSuccess() {
    // Given
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", "test-bucket");
    configs.put("storage.s3.region", localStack.getRegion());
    configs.put(
        "storage.s3.endpoint.override",
        localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

    System.setProperty("aws.accessKeyId", localStack.getAccessKey());
    System.setProperty("aws.secretAccessKey", localStack.getSecretKey());

    // S3 초기화
    S3Storage s3Storage = new S3Storage();
    s3Storage.configure(configs);

    String key = "topics/test-topic/data.json";
    String payload = "test";
    byte[] data = payload.getBytes(StandardCharsets.UTF_8);

    // When
    String resultUrl = s3Storage.store(key, data);

    // Then 1
    assertEquals("s3://test-bucket/topics/test-topic/data.json", resultUrl);

    // Then 2
    String storedContent =
        s3Client
            .getObjectAsBytes(GetObjectRequest.builder().bucket("test-bucket").key(key).build())
            .asUtf8String();

    assertEquals(payload, storedContent);
  }

  @Test
  @DisplayName("실패: 버킷이 존재하지 않으면 예외(RuntimeException)가 발생해야 한다")
  void storeFailNoBucket() {
    // Given
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", "no-bucket");
    configs.put("storage.s3.region", localStack.getRegion());
    configs.put(
        "s3.endpoint.override",
        localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

    System.setProperty("aws.accessKeyId", localStack.getAccessKey());
    System.setProperty("aws.secretAccessKey", localStack.getSecretKey());

    // S3 초기화
    S3Storage s3Storage = new S3Storage();
    s3Storage.configure(configs);

    String key = "connect/smt/data.json";
    String payload = "test";
    byte[] data = payload.getBytes(StandardCharsets.UTF_8);

    // When & Then
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> s3Storage.store(key, data));
    assertEquals("Failed to upload to S3. Bucket: no-bucket, Key: " + key, exception.getMessage());
  }
}
