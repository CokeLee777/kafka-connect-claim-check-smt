package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import static org.junit.jupiter.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchemaFields;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
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
class S3ClaimCheckSourceTransformIntegrationTest {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.2.0");

  @Container
  private static final LocalStackContainer localstack =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.S3);

  private static final String TEST_CONFIG_STORAGE_TYPE = "s3";
  private static final String TEST_CONFIG_THRESHOLD_BYTES = "100";
  private static final String TEST_CONFIG_BUCKET_NAME = "test-bucket";
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_LARGE_PAYLOAD = "this is large payload this is large payload!!!";
  private static final String TEST_SMALL_PAYLOAD = "small";

  private static S3Client s3Client;
  private ClaimCheckSourceTransform transform;
  private JsonConverter jsonConverter;

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
    transform = new ClaimCheckSourceTransform();
    jsonConverter = new JsonConverter();
    Map<String, Boolean> configs = Map.of("schema.enabled", true);
    jsonConverter.configure(configs, false);
  }

  private Map<String, String> createTransformConfig() {
    return Map.of(
        ClaimCheckSourceTransform.Config.STORAGE_TYPE,
        TEST_CONFIG_STORAGE_TYPE,
        ClaimCheckSourceTransform.Config.THRESHOLD_BYTES,
        TEST_CONFIG_THRESHOLD_BYTES,
        S3Storage.Config.BUCKET_NAME,
        TEST_CONFIG_BUCKET_NAME,
        S3Storage.Config.REGION,
        localstack.getRegion(),
        S3Storage.Config.ENDPOINT_OVERRIDE,
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
  }

  @Test
  @DisplayName("페이로드가 임계값을 초과하면 S3에 업로드하고 레코드를 참조로 변환한다")
  void shouldUploadToS3AndTransformRecordWhenPayloadExceedsThreshold() throws IOException {
    // Given
    transform.configure(createTransformConfig());
    byte[] largePayload = TEST_LARGE_PAYLOAD.getBytes(StandardCharsets.UTF_8);
    SourceRecord originalRecord =
        new SourceRecord(null, null, TEST_TOPIC, Schema.BYTES_SCHEMA, largePayload);
    byte[] serializedPayload =
        jsonConverter.fromConnectData(
            originalRecord.topic(), originalRecord.valueSchema(), originalRecord.value());

    // When
    SourceRecord transformedRecord = transform.apply(originalRecord);

    // Then
    assertNotNull(transformedRecord);
    assertNotEquals(originalRecord.value(), transformedRecord.value());
    assertEquals(ClaimCheckSchema.SCHEMA, transformedRecord.valueSchema());

    Struct transformedStruct = (Struct) transformedRecord.value();
    assertEquals(
        serializedPayload.length,
        transformedStruct.getInt64(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES));

    String referenceUrl = transformedStruct.getString(ClaimCheckSchemaFields.REFERENCE_URL);
    String key = referenceUrl.substring(("s3://" + TEST_CONFIG_BUCKET_NAME + "/").length());
    byte[] retrievedPayload =
        s3Client
            .getObject(GetObjectRequest.builder().bucket(TEST_CONFIG_BUCKET_NAME).key(key).build())
            .readAllBytes();
    assertArrayEquals(serializedPayload, retrievedPayload);
  }

  @Test
  @DisplayName("페이로드가 임계값 이하면 원본 레코드를 그대로 반환한다")
  void shouldReturnOriginalRecordWhenPayloadIsWithinThreshold() {
    // Given
    transform.configure(createTransformConfig());
    byte[] smallPayload = TEST_SMALL_PAYLOAD.getBytes(StandardCharsets.UTF_8);
    SourceRecord originalRecord =
        new SourceRecord(null, null, TEST_TOPIC, Schema.BYTES_SCHEMA, smallPayload);

    // When
    SourceRecord transformedRecord = transform.apply(originalRecord);

    // Then
    assertSame(originalRecord, transformedRecord);
    assertArrayEquals(smallPayload, (byte[]) transformedRecord.value());
  }

  @Test
  @DisplayName("null 페이로드는 그대로 반환한다")
  void shouldReturnOriginalRecordForNullPayload() {
    // Given
    transform.configure(createTransformConfig());
    SourceRecord originalRecord = new SourceRecord(null, null, TEST_TOPIC, null, null);

    // When
    SourceRecord transformedRecord = transform.apply(originalRecord);

    // Then
    assertSame(originalRecord, transformedRecord);
    assertNull(transformedRecord.value());
  }
}
