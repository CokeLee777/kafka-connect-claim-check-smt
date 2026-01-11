package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.S3Storage;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class S3ClaimCheckSourceTransformTest {

  private ClaimCheckSourceTransform transform;
  private ClaimCheckStorage mockStorage;

  @BeforeEach
  void setUp() throws Exception {
    transform = new ClaimCheckSourceTransform();
    mockStorage = mock(ClaimCheckStorage.class);
  }

  @AfterEach
  void tearDown() {
    transform.close();
  }

  private void injectDependencies(ClaimCheckStorage storage, int threshold) throws Exception {
    // storage 필드 주입
    Field storageField = ClaimCheckSourceTransform.class.getDeclaredField("storage");
    storageField.setAccessible(true);
    storageField.set(transform, storage);

    // thresholdBytes 필드 주입
    Field thresholdField = ClaimCheckSourceTransform.class.getDeclaredField("thresholdBytes");
    thresholdField.setAccessible(true);
    thresholdField.set(transform, threshold);

    // JsonConverter 초기화 및 주입
    JsonConverter converter = new JsonConverter();
    Map<String, Object> config = new HashMap<>();
    config.put("schemas.enable", "false");
    config.put("converter.type", "value");
    converter.configure(config);

    Field jsonConverterField = ClaimCheckSourceTransform.class.getDeclaredField("jsonConverter");
    jsonConverterField.setAccessible(true);
    jsonConverterField.set(transform, converter);
  }

  @Test
  @DisplayName("설정 테스트: 기본값 및 S3 스토리지 생성 확인")
  void testConfigure() {
    // Given
    Map<String, String> configs = new HashMap<>();
    configs.put(S3Storage.CONFIG_BUCKET_NAME, "test-bucket");
    configs.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "S3");
    configs.put(ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, "5000");

    // When & Then
    assertDoesNotThrow(() -> transform.configure(configs));
  }

  @Test
  @DisplayName("설정 테스트: 지원하지 않는 스토리지 타입 예외 발생")
  void testConfigureInvalidStorage() {
    // Given
    Map<String, String> configs = new HashMap<>();
    configs.put(S3Storage.CONFIG_BUCKET_NAME, "test-bucket");
    configs.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "INVALID_TYPE");

    // When & Then
    assertThrows(ConfigException.class, () -> transform.configure(configs));
  }

  @Test
  @DisplayName("임계값 이하의 작은 데이터는 변환 없이 통과해야 한다")
  void testApplySmallPayload() throws Exception {
    // Given
    injectDependencies(mockStorage, 100);

    byte[] value = "small data".getBytes(StandardCharsets.UTF_8);
    SourceRecord record = new SourceRecord(null, null, "test-topic", 0, Schema.BYTES_SCHEMA, value);

    // When
    SourceRecord result = transform.apply(record);

    // Then
    assertSame(record, result, "레코드 객체가 그대로 반환되어야 함");
    assertArrayEquals(value, (byte[]) result.value());
    verify(mockStorage, never()).store(anyString(), any());
  }

  @Test
  @DisplayName("임계값 초과 데이터는 스토리지에 저장하고 URL로 교체되어야 한다 (byte[])")
  void testApplyLargePayloadBytes() throws Exception {
    // Given
    injectDependencies(mockStorage, 1);

    String originalData = "Large payload";
    byte[] value = originalData.getBytes(StandardCharsets.UTF_8);
    String expectedUrl = "s3://bucket/topics/test-topic/9999/12/31/test.json";

    // When
    when(mockStorage.store(anyString(), any(byte[].class))).thenReturn(expectedUrl);

    SourceRecord record = new SourceRecord(null, null, "test-topic", 0, Schema.BYTES_SCHEMA, value);
    SourceRecord result = transform.apply(record);

    // Then
    assertTrue(result.value() instanceof Struct);
    Struct resultStruct = (Struct) result.value();
    assertEquals(expectedUrl, resultStruct.getString("reference_url"));
    verify(mockStorage).store(anyString(), eq(value));
  }

  @Test
  @DisplayName("String 타입의 큰 데이터도 처리되어야 한다")
  void testApplyLargePayloadString() throws Exception {
    // Given
    injectDependencies(mockStorage, 1);

    String value = "Large payload";
    String expectedUrl = "s3://bucket/topics/test-topic/9999/12/31/test.json";

    // When
    when(mockStorage.store(anyString(), any(byte[].class))).thenReturn(expectedUrl);

    SourceRecord record =
        new SourceRecord(null, null, "test-topic", 0, Schema.STRING_SCHEMA, value);

    SourceRecord result = transform.apply(record);

    // Then
    assertTrue(result.value() instanceof Struct);
    Struct resultStruct = (Struct) result.value();
    assertEquals(expectedUrl, resultStruct.getString("reference_url"));
    verify(mockStorage).store(anyString(), eq(value.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  @DisplayName("Struct 타입의 큰 데이터도 처리되어야 한다")
  void testApplyLargePayloadStruct() throws Exception {
    // Given
    injectDependencies(mockStorage, 1);

    Schema originalSchema = SchemaBuilder.struct()
            .name("com.example.OriginalUser")
            .field("username", Schema.STRING_SCHEMA)
            .build();
    Struct originalStruct = new Struct(originalSchema)
            .put("username", "cokelee777_is_testing");

    String expectedUrl = "s3://bucket/topics/test-topic/9999/12/31/test.json";

    // When
    when(mockStorage.store(anyString(), any(byte[].class))).thenReturn(expectedUrl);

    SourceRecord record =
            new SourceRecord(null, null, "test-topic", 0, originalSchema, originalStruct);

    SourceRecord result = transform.apply(record);

    // Then
    assertTrue(result.value() instanceof Struct, "결과값은 Struct 타입이어야 합니다.");
    Struct resultStruct = (Struct) result.value();
    assertEquals(
            ClaimCheckSourceTransform.REFERENCE_SCHEMA_NAME,
            result.valueSchema().name(),
            "스키마 이름이 ClaimCheckReference여야 합니다."
    );

    assertEquals(expectedUrl, resultStruct.getString("reference_url"));
    assertTrue(resultStruct.getInt64("original_size_bytes") > 1);

    ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(mockStorage).store(anyString(), bytesCaptor.capture());

    String uploadedContent = new String(bytesCaptor.getValue(), StandardCharsets.UTF_8);
    assertTrue(uploadedContent.contains("cokelee777_is_testing"), "업로드된 JSON에 원본 데이터가 포함되어야 합니다.");
  }

  @Test
  @DisplayName("S3 Key 생성 로직 검증 (토픽, 날짜, 확장자)")
  void testGeneratedKeyFormat() throws Exception {
    // Given
    injectDependencies(mockStorage, 1);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    when(mockStorage.store(keyCaptor.capture(), any())).thenReturn("url");

    Schema jsonSchema = SchemaBuilder.string().name("some.json.schema").build();
    long fixedTimestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC

    SourceRecord jsonRecord =
        new SourceRecord(
            null,
            null,
            "test-topic",
            0,
            Schema.STRING_SCHEMA,
            "key",
            jsonSchema,
            "payload-data",
            fixedTimestamp);

    // When
    transform.apply(jsonRecord);

    // Then
    String capturedKey = keyCaptor.getValue();
    assertTrue(capturedKey.startsWith("topics/test-topic/2024/01/01/"));
    assertTrue(capturedKey.endsWith(".txt"));
  }

  @Test
  @DisplayName("지원하지 않는 값 타입(List)은 무시하고 통과해야 한다")
  void testUnsupportedValueType() throws Exception {
    // Given
    injectDependencies(mockStorage, 1);

    List<String> value = Collections.singletonList("value");
    SourceRecord record = new SourceRecord(null, null, "test-topic", 0, null, value);

    // When
    SourceRecord result = transform.apply(record);

    // Then
    assertSame(record, result);
    verify(mockStorage, never()).store(anyString(), any());
  }

  @Test
  @DisplayName("Value가 null인 레코드는 그대로 반환해야 한다")
  void testNullValue() throws Exception {
    // Given
    injectDependencies(mockStorage, 1);

    SourceRecord record = new SourceRecord(null, null, "test-topic", 0, null, null);

    // When
    SourceRecord result = transform.apply(record);

    // Then
    assertSame(record, result);
  }
}
