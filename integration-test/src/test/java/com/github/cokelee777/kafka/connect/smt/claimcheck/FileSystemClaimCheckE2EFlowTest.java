package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type.FileSystemStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

@DisplayName("FileSystem Claim Check SMT E2E 통합 테스트")
class FileSystemClaimCheckE2EFlowTest {

  private static final String TOPIC_NAME = "test-topic";
  private static Path tempDirPath;

  private ClaimCheckSourceTransform sourceTransform;
  private ClaimCheckSinkTransform sinkTransform;

  @BeforeAll
  static void beforeAll() throws IOException {
    tempDirPath = Files.createTempDirectory("file-system-claim-check-test");
  }

  @AfterAll
  static void afterAll() throws IOException {
    // Delete the temporary directory and its contents
    if (tempDirPath != null && Files.exists(tempDirPath)) {
      try (Stream<Path> pathStream = Files.walk(tempDirPath)) {
        pathStream
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    System.err.println("Failed to delete " + path + ": " + e.getMessage());
                  }
                });
      }
    }
  }

  @BeforeEach
  void beforeEach() {
    sourceTransform = new ClaimCheckSourceTransform();
    sinkTransform = new ClaimCheckSinkTransform();
  }

  @AfterEach
  void afterEach() {
    sinkTransform.close();
    sourceTransform.close();
  }

  @Nested
  @DisplayName("정상 Flow 통합 테스트")
  class NormalFlowIntegrationTest {

    @Test
    @DisplayName("Sink -> Source 전체 흐름에서 메시지가 정상적으로 변환되고 원복되어야 한다.")
    void shouldPerformClaimCheckE2EFlow() throws IOException {
      /** Given: Common */
      // Common config
      Map<String, Object> commonConfig = new HashMap<>();
      commonConfig.put(FileSystemStorage.Config.PATH, tempDirPath.toString());

      /** Given: Source */
      // ClaimCheckSourceTransform config
      Map<String, Object> sourceTransformConfig = new HashMap<>(commonConfig);
      sourceTransformConfig.put(ClaimCheckSourceTransform.Config.THRESHOLD_BYTES, 1);
      sourceTransformConfig.put(
          ClaimCheckSourceTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.FILESYSTEM.type());
      sourceTransform.configure(sourceTransformConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();

      /** When: Source */
      SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

      /** Then: Source */
      Header transformedSourceHeader =
          validateTransformedSourceRecord(transformedSourceRecord, initialSourceRecord);

      /** Given: Sink */
      // ClaimCheckSinkTransform config
      Map<String, Object> sinkTransformConfig = new HashMap<>(commonConfig);
      sinkTransformConfig.put(
          ClaimCheckSinkTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.FILESYSTEM.type());
      sinkTransform.configure(sinkTransformConfig);

      SinkRecord initialSinkRecord =
          generateSinkRecord(transformedSourceRecord, transformedSourceHeader);

      /** When: Sink */
      SinkRecord restoredSinkRecord = sinkTransform.apply(initialSinkRecord);

      /** Then: Sink */
      validateRestoredSinkRecord(restoredSinkRecord, initialSourceRecord);
    }
  }

  @Nested
  @DisplayName("FileSystem SourceTransform 재시도 통합 테스트")
  class FileSystemSourceRetryIntegrationTest {

    @Test
    @DisplayName("일시적인 I/O 오류 발생 시 재시도하여 성공해야 한다")
    void shouldRetryAndSucceedOnTransientFailure() {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithRetry(3);
      sourceTransform.configure(sourceTransformConfig);
      SourceRecord initialSourceRecord = generateSourceRecord();

      AtomicInteger writeAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // 첫 번째 시도는 실패, 두 번째 시도는 성공하도록 설정
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenAnswer(
                invocation -> {
                  int attempt = writeAttemptCount.incrementAndGet();
                  if (attempt == 1) {
                    throw new IOException("Transient I/O error");
                  }
                  return invocation.callRealMethod();
                });

        // When
        SourceRecord transformedRecord = sourceTransform.apply(initialSourceRecord);

        // Then
        assertThat(transformedRecord).isNotNull();
        assertThat(writeAttemptCount.get()).isGreaterThanOrEqualTo(2);
      }
    }

    @Test
    @DisplayName("최대 재시도 횟수를 초과하면 예외가 발생해야 한다")
    void shouldFailWhenMaxRetriesExceeded() {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithRetry(2);
      sourceTransform.configure(sourceTransformConfig);
      SourceRecord initialSourceRecord = generateSourceRecord();

      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // 모든 시도가 실패하도록 설정
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenThrow(new IOException("Persistent I/O error"));

        // When & Then
        assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
            .isInstanceOf(RuntimeException.class);
      }
    }

    @Test
    @DisplayName("재시도 설정이 0일 때 즉시 실패해야 한다")
    void shouldFailImmediatelyWhenRetryDisabled() {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithRetry(0);
      sourceTransform.configure(sourceTransformConfig);
      SourceRecord initialSourceRecord = generateSourceRecord();

      AtomicInteger writeAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // 모든 시도가 실패하도록 설정
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenAnswer(
                invocation -> {
                  writeAttemptCount.incrementAndGet();
                  throw new IOException("I/O error");
                });

        // When & Then
        assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
            .isInstanceOf(RuntimeException.class);
        // 재시도 없이 1번만 시도해야 함
        assertThat(writeAttemptCount.get()).isEqualTo(1);
      }
    }
  }

  @Nested
  @DisplayName("FileSystem SinkTransform 재시도 통합 테스트")
  class FileSystemSinkRetryIntegrationTest {

    @Test
    @DisplayName("일시적인 I/O 오류 발생 시 재시도하여 성공해야 한다")
    void shouldRetryAndSucceedOnTransientFailure() throws IOException {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithRetry(3);
      sinkTransform.configure(sinkTransformConfig);
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      AtomicInteger readAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // 첫 번째 시도는 실패, 두 번째 시도는 성공하도록 설정
        mockedFiles
            .when(() -> Files.readAllBytes(any(Path.class)))
            .thenAnswer(
                invocation -> {
                  int attempt = readAttemptCount.incrementAndGet();
                  if (attempt == 1) {
                    throw new IOException("Transient I/O error");
                  }
                  return invocation.callRealMethod();
                });

        // When
        SinkRecord restoredSinkRecord = sinkTransform.apply(initialSinkRecord);

        // Then
        assertThat(restoredSinkRecord).isNotNull();
        assertThat(readAttemptCount.get()).isGreaterThanOrEqualTo(2);
      }
    }

    @Test
    @DisplayName("최대 재시도 횟수를 초과하면 예외가 발생해야 한다")
    void shouldFailWhenMaxRetriesExceeded() throws IOException {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithRetry(2);
      sinkTransform.configure(sinkTransformConfig);
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // 모든 시도가 실패하도록 설정
        mockedFiles
            .when(() -> Files.readAllBytes(any(Path.class)))
            .thenThrow(new IOException("Persistent I/O error"));

        // When & Then
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> sinkTransform.apply(initialSinkRecord))
            .withMessageStartingWith("Failed to read claim check file:");
      }
    }

    @Test
    @DisplayName("재시도 설정이 0일 때 즉시 실패해야 한다")
    void shouldFailImmediatelyWhenRetryDisabled() throws IOException {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithRetry(0);
      sinkTransform.configure(sinkTransformConfig);
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      AtomicInteger readAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // 모든 시도가 실패하도록 설정
        mockedFiles
            .when(() -> Files.readAllBytes(any(Path.class)))
            .thenAnswer(
                invocation -> {
                  readAttemptCount.incrementAndGet();
                  throw new IOException("I/O error");
                });

        // When & Then
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> sinkTransform.apply(initialSinkRecord))
            .withMessageStartingWith("Failed to read claim check file:");
        // 재시도 없이 1번만 시도해야 함
        assertThat(readAttemptCount.get()).isEqualTo(1);
      }
    }

    private SinkRecord storeDataAndCreateSinkRecord() {
      Map<String, Object> sourceConfig = new HashMap<>();
      sourceConfig.put(
          ClaimCheckSourceTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.FILESYSTEM.type());
      sourceConfig.put(ClaimCheckSourceTransform.Config.THRESHOLD_BYTES, 1);
      sourceConfig.put(FileSystemStorage.Config.PATH, tempDirPath.toString());
      sourceTransform.configure(sourceConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();
      SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

      Header transformedSourceHeader =
          transformedSourceRecord.headers().lastWithName(ClaimCheckSchema.NAME);

      return generateSinkRecord(transformedSourceRecord, transformedSourceHeader);
    }
  }

  private Map<String, Object> generateSourceConfigWithRetry(int retryMax) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        ClaimCheckSourceTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.FILESYSTEM.type());
    config.put(ClaimCheckSourceTransform.Config.THRESHOLD_BYTES, 1);
    config.put(FileSystemStorage.Config.PATH, tempDirPath.toString());
    config.put(FileSystemStorage.Config.RETRY_MAX, retryMax);
    config.put(FileSystemStorage.Config.RETRY_BACKOFF_MS, 50L);
    config.put(FileSystemStorage.Config.RETRY_MAX_BACKOFF_MS, 100L);
    return config;
  }

  private Map<String, Object> generateSinkConfigWithRetry(int retryMax) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        ClaimCheckSinkTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.FILESYSTEM.type());
    config.put(FileSystemStorage.Config.PATH, tempDirPath.toString());
    config.put(FileSystemStorage.Config.RETRY_MAX, retryMax);
    config.put(FileSystemStorage.Config.RETRY_BACKOFF_MS, 50L);
    config.put(FileSystemStorage.Config.RETRY_MAX_BACKOFF_MS, 100L);
    return config;
  }

  private SourceRecord generateSourceRecord() {
    Schema schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("id", 1L).put("name", "cokelee777");
    return new SourceRecord(null, null, TOPIC_NAME, null, null, schema, value);
  }

  private SinkRecord generateSinkRecord(
      SourceRecord transformedSourceRecord, Header transformedSourceHeader) {
    SinkRecord sinkRecord =
        new SinkRecord(
            transformedSourceRecord.topic(),
            0,
            transformedSourceRecord.keySchema(),
            transformedSourceRecord.key(),
            transformedSourceRecord.valueSchema(),
            transformedSourceRecord.value(),
            0);
    sinkRecord.headers().add(transformedSourceHeader);
    return sinkRecord;
  }

  private Header validateTransformedSourceRecord(
      SourceRecord transformedSourceRecord, SourceRecord initialSourceRecord) throws IOException {
    // ClaimCheckSourceRecord 검증
    assertThat(transformedSourceRecord).isNotNull();
    assertThat(transformedSourceRecord.topic()).isEqualTo(TOPIC_NAME);
    assertThat(transformedSourceRecord.keySchema()).isNull();
    assertThat(transformedSourceRecord.key()).isNull();
    assertThat(transformedSourceRecord.valueSchema()).isEqualTo(initialSourceRecord.valueSchema());
    assertThat(transformedSourceRecord.value()).isNotNull();
    assertThat(transformedSourceRecord.value()).isInstanceOf(Struct.class);
    assertThat(transformedSourceRecord.value()).isNotEqualTo(initialSourceRecord.value());

    // GenericStructStrategy 적용되어 모든 필드가 기본값으로 설정됨
    assertThat(((Struct) transformedSourceRecord.value()).getInt64("id")).isEqualTo(0L);
    assertThat(((Struct) transformedSourceRecord.value()).getString("name")).isEqualTo("");

    // ClaimCheckSourceHeader 검증
    Header transformedSourceHeader =
        transformedSourceRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(transformedSourceHeader).isNotNull();
    assertThat(transformedSourceHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
    assertThat(transformedSourceHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    assertThat(transformedSourceHeader.value()).isInstanceOf(Struct.class);

    // 실제 데이터 검증
    ClaimCheckValue claimCheckValue = ClaimCheckValue.from(transformedSourceHeader.value());
    String referenceUrl = claimCheckValue.referenceUrl();
    int originalSizeBytes = claimCheckValue.originalSizeBytes();

    assertThat(referenceUrl).startsWith("file://" + tempDirPath.toRealPath() + "/");
    assertThat(originalSizeBytes).isGreaterThan(0);

    // 파일 시스템에 실제 데이터가 저장되었는지 확인
    Path filePath = Path.of(referenceUrl.replace("file://", ""));
    assertThat(Files.exists(filePath)).isTrue();
    assertThat(Files.readAllBytes(filePath)).isNotEmpty();
    assertThat(Files.readAllBytes(filePath).length).isEqualTo(originalSizeBytes);

    return transformedSourceHeader;
  }

  private void validateRestoredSinkRecord(
      SinkRecord restoredSinkRecord, SourceRecord initialSourceRecord) {
    assertThat(restoredSinkRecord).isNotNull();
    assertThat(restoredSinkRecord.topic()).isEqualTo(TOPIC_NAME);
    assertThat(restoredSinkRecord.keySchema()).isNull();
    assertThat(restoredSinkRecord.key()).isNull();
    assertThat(restoredSinkRecord.valueSchema()).isEqualTo(initialSourceRecord.valueSchema());
    assertThat(restoredSinkRecord.value()).isNotNull();
    assertThat(restoredSinkRecord.value()).isInstanceOf(Struct.class);

    // 복원된 값이 원본과 동일한지 검증
    Struct restoredValue = (Struct) restoredSinkRecord.value();
    assertThat(restoredValue.getInt64("id")).isEqualTo(1L);
    assertThat(restoredValue.getString("name")).isEqualTo("cokelee777");
    assertThat(restoredValue).isEqualTo(initialSourceRecord.value());

    // ClaimCheck 헤더가 제거되었는지 확인
    Header claimCheckSinkHeader = restoredSinkRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(claimCheckSinkHeader).isNull();
  }
}
