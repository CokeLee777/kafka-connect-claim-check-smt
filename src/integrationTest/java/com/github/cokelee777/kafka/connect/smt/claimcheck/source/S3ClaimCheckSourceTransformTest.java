package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
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
@DisplayName("S3ClaimCheckSourceTransform 통합 테스트")
class S3ClaimCheckSourceTransformTest {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.2.0");
  private static final String BUCKET_NAME = "test-bucket";

  @Container
  private static final LocalStackContainer localstack =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.S3);

  private static S3Client s3Client;
  private ClaimCheckSourceTransform transform;

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
    transform = new ClaimCheckSourceTransform();
  }

  private Map<String, String> createTransformConfig(int threshold) {
    Map<String, String> config = new HashMap<>();
    config.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "S3");
    config.put(ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, String.valueOf(threshold));
    config.put("storage.s3.bucket.name", BUCKET_NAME);
    config.put("storage.s3.region", localstack.getRegion());
    config.put(
        "storage.s3.endpoint.override",
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    config.put("storage.s3.credentials.access.key.id", localstack.getAccessKey());
    config.put("storage.s3.credentials.secret.access.key", localstack.getSecretKey());
    return config;
  }

  @Test
  @DisplayName("페이로드가 임계값을 초과하면 S3에 업로드하고 레코드를 참조로 변환한다")
  void shouldUploadToS3AndTransformRecordWhenPayloadExceedsThreshold() throws IOException {
    // Given
    transform.configure(createTransformConfig(20));
    byte[] largePayload =
        "this is a very large payload that exceeds the threshold".getBytes(StandardCharsets.UTF_8);
    SourceRecord record =
        new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, largePayload);

    // When
    SourceRecord transformedRecord = transform.apply(record);

    // Then
    assertNotNull(transformedRecord);
    assertNotEquals(record.value(), transformedRecord.value());
    assertEquals(ClaimCheckSourceTransform.REFERENCE_SCHEMA, transformedRecord.valueSchema());

    Struct reference = (Struct) transformedRecord.value();
    String s3uri = reference.getString("reference_url");
    assertEquals(largePayload.length, reference.getInt64("original_size_bytes"));

    String key = s3uri.substring(("s3://" + BUCKET_NAME + "/").length());
    byte[] storedData =
        s3Client
            .getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build())
            .readAllBytes();
    assertArrayEquals(largePayload, storedData);
  }

  @Test
  @DisplayName("페이로드가 임계값 이하면 원본 레코드를 그대로 반환한다")
  void shouldReturnOriginalRecordWhenPayloadIsWithinThreshold() {
    // Given
    transform.configure(createTransformConfig(100));
    byte[] smallPayload = "small payload".getBytes(StandardCharsets.UTF_8);
    SourceRecord record =
        new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, smallPayload);

    // When
    SourceRecord transformedRecord = transform.apply(record);

    // Then
    assertSame(record, transformedRecord);
    assertArrayEquals(smallPayload, (byte[]) transformedRecord.value());
  }

  @Test
  @DisplayName("null 페이로드는 그대로 반환한다")
  void shouldReturnOriginalRecordForNullPayload() {
    // Given
    transform.configure(createTransformConfig(100));
    SourceRecord record = new SourceRecord(null, null, "test-topic", null, null);

    // When
    SourceRecord transformedRecord = transform.apply(record);

    // Then
    assertSame(record, transformedRecord);
    assertNull(transformedRecord.value());
  }
}
