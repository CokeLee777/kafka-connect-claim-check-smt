package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3ClaimCheckSourceTransform 단위 테스트")
public class S3ClaimCheckSourceTransformTest {

  private ClaimCheckSourceTransform transform;

  @Mock private ClaimCheckStorage storage;

  private static final String TOPIC = "test-topic";
  private static final int PARTITION = 0;
  private static final String S3_URI = "s3://test-bucket/claim-checks/some-uuid";

  @BeforeEach
  void setUp() {
    transform =
        new ClaimCheckSourceTransform() {
          @Override
          protected ClaimCheckStorage initStorage(String type) {
            if ("S3".equalsIgnoreCase(type)) {
              return storage;
            }
            return super.initStorage(type);
          }
        };
  }

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTests {

    @Nested
    @DisplayName("성공 케이스")
    class Success {
      @Test
      @DisplayName("필수 설정만으로 정상적으로 초기화된다")
      void shouldConfigureWithDefaultProps() {
        // Given
        Map<String, String> props = new HashMap<>();
        props.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "S3");

        // When
        transform.configure(props);

        // Then
        verify(storage, times(1)).configure(props);
      }

      @Test
      @DisplayName("임계값 설정이 포함된 경우 정상적으로 초기화된다")
      void shouldConfigureWithThreshold() {
        // Given
        Map<String, String> props = new HashMap<>();
        props.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "S3");
        props.put(ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, "100");

        // When
        transform.configure(props);

        // Then
        verify(storage, times(1)).configure(props);
      }
    }

    @Nested
    @DisplayName("실패 케이스")
    class Failure {

      @Test
      @DisplayName("지원하지 않는 storage.type이면 ConfigException이 발생한다")
      void shouldThrowExceptionForUnsupportedStorageType() {
        // Given
        Map<String, String> props = new HashMap<>();
        props.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "UnsupportedDB");

        // When & Then
        ConfigException exception =
            assertThrows(ConfigException.class, () -> transform.configure(props));
        assertEquals("Unsupported storage type: UnsupportedDB", exception.getMessage());
      }
    }
  }

  @Nested
  @DisplayName("apply 메서드 테스트")
  class ApplyTests {

    @BeforeEach
    void applySetup() {
      Map<String, String> props = new HashMap<>();
      props.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "S3");
      props.put(ClaimCheckSourceTransform.CONFIG_THRESHOLD_BYTES, "10"); // 10 바이트로 설정
      transform.configure(props);
    }

    @Nested
    @DisplayName("Claim Check 수행 케이스")
    class ClaimCheckPerformCases {

      @Test
      @DisplayName("byte[] 값이 임계값을 초과하면 Claim Check을 수행한다")
      void shouldPerformClaimCheckForByteArray() {
        // Given
        byte[] largeValue = "this is a large value".getBytes(StandardCharsets.UTF_8);
        SourceRecord record =
            new SourceRecord(
                null, null, TOPIC, PARTITION, null, null, Schema.BYTES_SCHEMA, largeValue);
        when(storage.store(largeValue)).thenReturn(S3_URI);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        verify(storage, times(1)).store(largeValue);
        assertNotSame(record, transformedRecord);
        assertEquals(ClaimCheckSourceTransform.REFERENCE_SCHEMA, transformedRecord.valueSchema());
        Struct reference = (Struct) transformedRecord.value();
        assertEquals(S3_URI, reference.getString("reference_url"));
        assertEquals(largeValue.length, reference.getInt64("original_size_bytes"));
        assertNotNull(reference.getInt64("uploaded_at"));
      }

      @Test
      @DisplayName("String 값이 임계값을 초과하면 Claim Check을 수행한다")
      void shouldPerformClaimCheckForString() {
        // Given
        String largeValue = "this is a large string value";
        SourceRecord record =
            new SourceRecord(
                null, null, TOPIC, PARTITION, null, null, Schema.STRING_SCHEMA, largeValue);
        byte[] largeValueBytes = largeValue.getBytes(StandardCharsets.UTF_8);
        when(storage.store(largeValueBytes)).thenReturn(S3_URI);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        verify(storage, times(1)).store(largeValueBytes);
        Struct reference = (Struct) transformedRecord.value();
        assertEquals(S3_URI, reference.getString("reference_url"));
        assertEquals(largeValueBytes.length, reference.getInt64("original_size_bytes"));
      }

      @Test
      @DisplayName("Struct 값이 임계값을 초과하면 Claim Check을 수행한다")
      void shouldPerformClaimCheckForStruct() {
        // Given
        Schema structSchema =
            SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .build();
        Struct largeValue =
            new Struct(structSchema).put("id", 1).put("data", "very long data string");
        SourceRecord record =
            new SourceRecord(null, null, TOPIC, PARTITION, null, null, structSchema, largeValue);

        // JSON 변환 후의 바이트 배열을 예상해야 함
        byte[] largeValueBytes =
            "{\"id\":1,\"data\":\"very long data string\"}".getBytes(StandardCharsets.UTF_8);
        when(storage.store(any(byte[].class))).thenReturn(S3_URI);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        verify(storage, times(1)).store(any(byte[].class));
        Struct reference = (Struct) transformedRecord.value();
        assertEquals(S3_URI, reference.getString("reference_url"));
      }
    }

    @Nested
    @DisplayName("Claim Check 미수행 케이스")
    class ClaimCheckSkipCases {

      @Test
      @DisplayName("값이 임계값 이하이면 원본 레코드를 그대로 반환한다")
      void shouldReturnOriginalRecordWhenValueIsWithinThreshold() {
        // Given
        byte[] smallValue = "small".getBytes(StandardCharsets.UTF_8);
        SourceRecord record =
            new SourceRecord(
                null, null, TOPIC, PARTITION, null, null, Schema.BYTES_SCHEMA, smallValue);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        verify(storage, never()).store(any());
        assertSame(record, transformedRecord);
      }

      @Test
      @DisplayName("값이 null이면 원본 레코드를 그대로 반환한다")
      void shouldReturnOriginalRecordWhenValueIsNull() {
        // Given
        SourceRecord record =
            new SourceRecord(null, null, TOPIC, PARTITION, null, null, null, null);

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
        Integer unsupportedValue = 123456789;
        SourceRecord record =
            new SourceRecord(
                null, null, TOPIC, PARTITION, null, null, Schema.INT32_SCHEMA, unsupportedValue);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        verify(storage, never()).store(any());
        assertSame(record, transformedRecord);
      }
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTests {

    @Test
    @DisplayName("close가 호출되면 storage의 close가 호출된다")
    void shouldCallStorageClose() {
      // Given
      Map<String, String> props = new HashMap<>();
      props.put(ClaimCheckSourceTransform.CONFIG_STORAGE_TYPE, "S3");
      transform.configure(props);
      doNothing().when(storage).close();

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
    }
  }
}
