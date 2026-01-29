package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem.FileSystemStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;

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
    String referenceUrl = claimCheckValue.getReferenceUrl();
    int originalSizeBytes = claimCheckValue.getOriginalSizeBytes();

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
