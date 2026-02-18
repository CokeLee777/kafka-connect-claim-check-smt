package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSinkTransform;
import com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSourceTransform;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;

class NormalFlowFileSystemIntegrationTest extends AbstractFileSystemIntegrationTest {

  private ClaimCheckSourceTransform sourceTransform;
  private ClaimCheckSinkTransform sinkTransform;

  @BeforeEach
  void setUp() {
    sourceTransform = new ClaimCheckSourceTransform();
    sinkTransform = new ClaimCheckSinkTransform();
  }

  @AfterEach
  void tearDown() {
    sinkTransform.close();
    sourceTransform.close();
  }

  @Test
  void shouldPerformClaimCheckE2EFlow() throws IOException {
    // Given: Common
    // Common config
    Map<String, Object> commonConfig = new HashMap<>();
    commonConfig.put(FileSystemStorageConfig.PATH_CONFIG, TEMP_DIR_PATH.toString());

    // Given: Source
    // ClaimCheckSourceTransform config
    Map<String, Object> sourceTransformConfig = new HashMap<>(commonConfig);
    sourceTransformConfig.put(ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, 1);
    sourceTransformConfig.put(
        ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG,
        ClaimCheckStorageType.FILESYSTEM.type());
    sourceTransform.configure(sourceTransformConfig);

    SourceRecord initialSourceRecord = generateSourceRecord();

    // When: Source
    SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

    // Then: Source
    Header transformedSourceHeader =
        validateTransformedSourceRecord(transformedSourceRecord, initialSourceRecord);

    // Given: Sink
    // ClaimCheckSinkTransform config
    Map<String, Object> sinkTransformConfig = new HashMap<>(commonConfig);
    sinkTransformConfig.put(
        ClaimCheckSinkTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.FILESYSTEM.type());
    sinkTransform.configure(sinkTransformConfig);

    SinkRecord initialSinkRecord =
        generateSinkRecord(transformedSourceRecord, transformedSourceHeader);

    // When: Sink
    SinkRecord restoredSinkRecord = sinkTransform.apply(initialSinkRecord);

    // Then: Sink
    validateRestoredSinkRecord(restoredSinkRecord, initialSourceRecord);
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
    // Validate ClaimCheckSourceRecord
    assertThat(transformedSourceRecord).isNotNull();
    assertThat(transformedSourceRecord.topic()).isEqualTo(TOPIC_NAME);
    assertThat(transformedSourceRecord.keySchema()).isNull();
    assertThat(transformedSourceRecord.key()).isNull();
    assertThat(transformedSourceRecord.valueSchema()).isEqualTo(initialSourceRecord.valueSchema());
    assertThat(transformedSourceRecord.value()).isNotNull();
    assertThat(transformedSourceRecord.value()).isInstanceOf(Struct.class);
    assertThat(transformedSourceRecord.value()).isNotEqualTo(initialSourceRecord.value());

    // GenericStructStrategy applied, all fields set to default values
    assertThat(((Struct) transformedSourceRecord.value()).getInt64("id")).isEqualTo(0L);
    assertThat(((Struct) transformedSourceRecord.value()).getString("name")).isEqualTo("");

    // Validate ClaimCheckSourceHeader
    Header transformedSourceHeader =
        transformedSourceRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
    assertThat(transformedSourceHeader).isNotNull();
    assertThat(transformedSourceHeader.key()).isEqualTo(ClaimCheckHeader.HEADER_KEY);
    assertThat(transformedSourceHeader.schema()).isEqualTo(Schema.STRING_SCHEMA);
    assertThat(transformedSourceHeader.value()).isInstanceOf(String.class);
    assertThatNoException()
        .isThrownBy(
            () -> {
              ClaimCheckMetadata claimCheckMetadata =
                  ClaimCheckHeader.fromHeader(transformedSourceHeader);
              assertThat(claimCheckMetadata).isNotNull();
              assertThat(claimCheckMetadata.referenceUrl()).isNotNull();
              assertThat(claimCheckMetadata.originalSizeBytes()).isNotZero();
              assertThat(claimCheckMetadata.uploadedAt()).isNotZero();
            });

    // Validate actual data
    ClaimCheckMetadata claimCheckMetadata = ClaimCheckHeader.fromHeader(transformedSourceHeader);
    String referenceUrl = claimCheckMetadata.referenceUrl();
    int originalSizeBytes = claimCheckMetadata.originalSizeBytes();

    assertThat(referenceUrl).startsWith("file://" + TEMP_DIR_PATH.toRealPath() + "/");
    assertThat(originalSizeBytes).isGreaterThan(0);

    // Verify that actual data is stored in file system
    Path filePath = Path.of(URI.create(referenceUrl).getPath());
    assertThat(Files.exists(filePath)).isTrue();
    byte[] fileContent = Files.readAllBytes(filePath);
    assertThat(fileContent).isNotEmpty();
    assertThat(fileContent.length).isEqualTo(originalSizeBytes);

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

    // Verify that restored value equals original
    Struct restoredValue = (Struct) restoredSinkRecord.value();
    assertThat(restoredValue.getInt64("id")).isEqualTo(1L);
    assertThat(restoredValue.getString("name")).isEqualTo("cokelee777");
    assertThat(restoredValue).isEqualTo(initialSourceRecord.value());

    // Verify that ClaimCheck header is removed
    Header claimCheckSinkHeader =
        restoredSinkRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);
    assertThat(claimCheckSinkHeader).isNull();
  }
}
