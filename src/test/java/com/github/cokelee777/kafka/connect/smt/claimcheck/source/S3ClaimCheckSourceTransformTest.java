package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchemaFields;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3ClaimCheckSourceTransform 단위 테스트")
public class S3ClaimCheckSourceTransformTest {

  private static final String TEST_CONFIG_STORAGE_TYPE = "s3";
  private static final String TEST_CONFIG_UNSUPPORTED_STORAGE_TYPE = "UnsupportedStorage";
  private static final String TEST_CONFIG_THRESHOLD_BYTES = "10";
  private static final String TEST_CONFIG_BUCKET_NAME = "test-bucket";
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_LARGE_PAYLOAD = "this is large payload !!!";
  private static final String TEST_SMALL_PAYLOAD = "small";
  private static final String TEST_REFERENCE_URL = "s3://test-bucket/claim-checks/some-uuid";

  private ClaimCheckSourceTransform transform;
  @Mock private ClaimCheckStorage storage;

  @BeforeEach
  void setUp() {
    transform = new ClaimCheckSourceTransform();
  }

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTests {

    private MockedStatic<ClaimCheckStorageFactory> factory;

    @BeforeEach
    void configureSetUp() {
      factory = mockStatic(ClaimCheckStorageFactory.class);
    }

    @AfterEach
    void configureTearDown() {
      factory.close();
    }

    @Nested
    @DisplayName("성공 케이스")
    class Success {

      @BeforeEach
      void configureSetUp() {
        factory
            .when(() -> ClaimCheckStorageFactory.create(TEST_CONFIG_STORAGE_TYPE))
            .thenReturn(storage);
      }

      @Test
      @DisplayName("필수 설정만으로 정상적으로 초기화된다")
      void shouldConfigureWithDefaultProps() {
        // Given
        Map<String, String> configs =
            Map.of(
                ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, TEST_CONFIG_STORAGE_TYPE,
                S3Storage.CONFIG_BUCKET_NAME, TEST_CONFIG_BUCKET_NAME);

        // When
        transform.configure(configs);

        // Then
        verify(storage, times(1)).configure(configs);
      }

      @Test
      @DisplayName("임계값 설정이 포함된 경우 정상적으로 초기화된다")
      void shouldConfigureWithThreshold() {
        // Given
        Map<String, String> configs =
            Map.of(
                ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, TEST_CONFIG_STORAGE_TYPE,
                ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, TEST_CONFIG_THRESHOLD_BYTES,
                S3Storage.CONFIG_BUCKET_NAME, TEST_CONFIG_BUCKET_NAME);

        // When
        transform.configure(configs);

        // Then
        verify(storage, times(1)).configure(configs);
      }
    }

    @Nested
    @DisplayName("실패 케이스")
    class Failure {

      @Test
      @DisplayName("storage 생성 중 예외가 발생하면 그대로 전파한다")
      void shouldPropagateExceptionFromFactory() {
        // given
        factory
            .when(() -> ClaimCheckStorageFactory.create(TEST_CONFIG_UNSUPPORTED_STORAGE_TYPE))
            .thenThrow(
                new ConfigException(
                    "Unsupported storage type: " + TEST_CONFIG_UNSUPPORTED_STORAGE_TYPE));

        Map<String, String> configs =
            Map.of(
                ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE,
                TEST_CONFIG_UNSUPPORTED_STORAGE_TYPE);

        // when & then
        assertThrows(ConfigException.class, () -> transform.configure(configs));
      }
    }
  }

  @Nested
  @DisplayName("apply 메서드 테스트")
  class ApplyTests {

    private MockedStatic<ClaimCheckStorageFactory> factory;

    @BeforeEach
    void applySetUp() {
      factory = mockStatic(ClaimCheckStorageFactory.class);
      factory
          .when(() -> ClaimCheckStorageFactory.create(TEST_CONFIG_STORAGE_TYPE))
          .thenReturn(storage);

      Map<String, String> configs = new HashMap<>();
      configs.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, TEST_CONFIG_STORAGE_TYPE);
      configs.put(S3Storage.CONFIG_BUCKET_NAME, TEST_CONFIG_BUCKET_NAME);
      configs.put(ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, TEST_CONFIG_THRESHOLD_BYTES);
      transform.configure(configs);
    }

    @AfterEach
    void applyTearDown() {
      factory.close();
    }

    @Nested
    @DisplayName("Claim Check 수행 케이스")
    class ClaimCheckPerformCases {

      @Test
      @DisplayName("byte[] payload가 threshold를 초과하면 storage에 저장하고 reference로 치환한다")
      void shouldPerformClaimCheckForByteArray() {
        // Given
        SourceRecord originalRecord =
            new SourceRecord(
                null,
                null,
                TEST_TOPIC,
                0,
                null,
                null,
                Schema.BYTES_SCHEMA,
                TEST_LARGE_PAYLOAD.getBytes(StandardCharsets.UTF_8));
        when(storage.store(any(byte[].class))).thenReturn(TEST_REFERENCE_URL);

        // When
        SourceRecord transformedRecord = transform.apply(originalRecord);

        // Then
        byte[] serializedValue = verifyStoredOnce();
        assertClaimChecked(originalRecord, transformedRecord, serializedValue.length);
      }

      @Test
      @DisplayName("String payload가 threshold를 초과하면 storage에 저장하고 reference로 치환한다")
      void shouldPerformClaimCheckForString() {
        // Given
        SourceRecord originalRecord =
            new SourceRecord(
                null, null, TEST_TOPIC, 0, null, null, Schema.STRING_SCHEMA, TEST_LARGE_PAYLOAD);
        when(storage.store(any(byte[].class))).thenReturn(TEST_REFERENCE_URL);

        // When
        SourceRecord transformedRecord = transform.apply(originalRecord);

        // Then
        byte[] serializedValue = verifyStoredOnce();
        assertClaimChecked(originalRecord, transformedRecord, serializedValue.length);
      }

      @Test
      @DisplayName("Struct payload가 threshold를 초과하면 storage에 저장하고 reference로 치환한다")
      void shouldPerformClaimCheckForStruct() {
        // Given
        Schema structSchema =
            SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Struct struct = new Struct(structSchema).put("id", 1).put("name", TEST_LARGE_PAYLOAD);
        SourceRecord originalRecord =
            new SourceRecord(null, null, TEST_TOPIC, 0, null, null, structSchema, struct);
        when(storage.store(any(byte[].class))).thenReturn(TEST_REFERENCE_URL);

        // When
        SourceRecord transformedRecord = transform.apply(originalRecord);

        // Then
        byte[] serializedValue = verifyStoredOnce();
        assertClaimChecked(originalRecord, transformedRecord, serializedValue.length);
      }

      private byte[] verifyStoredOnce() {
        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        verify(storage, times(1)).store(captor.capture());
        return captor.getValue();
      }

      private void assertClaimChecked(
          SourceRecord originalRecord, SourceRecord transformedRecord, long originalSizeBytes) {
        assertNotSame(originalRecord, transformedRecord);
        assertEquals(ClaimCheckSchema.SCHEMA, transformedRecord.valueSchema());

        Struct transformedStruct = (Struct) transformedRecord.value();
        assertEquals(
            TEST_REFERENCE_URL, transformedStruct.getString(ClaimCheckSchemaFields.REFERENCE_URL));
        assertEquals(
            originalSizeBytes, transformedStruct.getInt64(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES));
        assertNotNull(transformedStruct.getInt64(ClaimCheckSchemaFields.UPLOADED_AT));
      }
    }

    @Nested
    @DisplayName("Claim Check 미수행 케이스")
    class ClaimCheckSkipCases {

      @Test
      @DisplayName("값이 임계값 이하이면 원본 레코드를 그대로 반환한다")
      void shouldReturnOriginalRecordWhenValueIsWithinThreshold() {
        // Given
        SourceRecord original =
            new SourceRecord(
                null,
                null,
                TEST_TOPIC,
                0,
                null,
                null,
                Schema.BYTES_SCHEMA,
                TEST_SMALL_PAYLOAD.getBytes(StandardCharsets.UTF_8));

        // When
        SourceRecord transformedRecord = transform.apply(original);

        // Then
        verify(storage, never()).store(any());
        assertSame(original, transformedRecord);
      }

      @Test
      @DisplayName("값이 null이면 원본 레코드를 그대로 반환한다")
      void shouldReturnOriginalRecordWhenValueIsNull() {
        // Given
        SourceRecord record = new SourceRecord(null, null, TEST_TOPIC, 0, null, null, null, null);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        verify(storage, never()).store(any());
        assertSame(record, transformedRecord);
      }

      @Test
      @DisplayName("지원하지 않는 타입의 값이면 원본 레코드를 그대로 반환한다")
      void shouldReturnOriginalRecordForUnsupportedType() {
        // Given
        Integer unsupportedTypeValue = 123456789;
        SourceRecord originalRecord =
            new SourceRecord(
                null, null, TEST_TOPIC, 0, null, null, Schema.INT32_SCHEMA, unsupportedTypeValue);

        // When
        SourceRecord transformedRecord = transform.apply(originalRecord);

        // Then
        verify(storage, never()).store(any());
        assertSame(originalRecord, transformedRecord);
      }
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTests {

    private MockedStatic<ClaimCheckStorageFactory> factory;

    @BeforeEach
    void closeSetUp() {
      factory = mockStatic(ClaimCheckStorageFactory.class);
      factory
          .when(() -> ClaimCheckStorageFactory.create(TEST_CONFIG_STORAGE_TYPE))
          .thenReturn(storage);
    }

    @AfterEach
    void closeTearDown() {
      factory.close();
    }

    @Test
    @DisplayName("close가 호출되면 storage의 close가 호출된다")
    void shouldCallStorageClose() {
      // Given
      Map<String, String> configs = new HashMap<>();
      configs.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, TEST_CONFIG_STORAGE_TYPE);
      configs.put(S3Storage.CONFIG_BUCKET_NAME, TEST_CONFIG_BUCKET_NAME);
      configs.put(ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, TEST_CONFIG_THRESHOLD_BYTES);
      transform.configure(configs);

      // When
      transform.close();

      // Then
      verify(storage, times(1)).close();
    }

    @Test
    @DisplayName("configure가 호출되지 않았어도 close는 예외를 발생시키지 않는다")
    void shouldNotThrowExceptionOnCloseWhenNotConfigured() {
      // When & Then
      assertDoesNotThrow(() -> transform.close());
      verify(storage, never()).close();
    }
  }
}
