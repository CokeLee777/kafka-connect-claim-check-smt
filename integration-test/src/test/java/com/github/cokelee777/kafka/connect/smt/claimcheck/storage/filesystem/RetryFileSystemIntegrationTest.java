package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSinkTransform;
import com.github.cokelee777.kafka.connect.smt.claimcheck.ClaimCheckSourceTransform;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckHeader;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckMetadata;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors.ClaimCheckRetrieveException;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors.ClaimCheckStoreException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class RetryFileSystemIntegrationTest extends AbstractFileSystemIntegrationTest {

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

  @Nested
  class FileSystemSourceRetryIntegrationTest {

    @Test
    void shouldSucceedWithRetriesWhenFileWriteFailsTemporarily() throws IOException {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithRetry(3);
      sourceTransform.configure(sourceTransformConfig);
      SourceRecord initialSourceRecord = generateSourceRecord();

      AtomicInteger writeAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // First attempt fails, second attempt succeeds
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

        validateTransformedSourceRecord(transformedRecord, initialSourceRecord);
      }
    }

    @Test
    void shouldFailWhenFileWriteFailsPersistently() {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithRetry(2);
      sourceTransform.configure(sourceTransformConfig);
      SourceRecord initialSourceRecord = generateSourceRecord();

      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // All attempts fail
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenThrow(new IOException("Persistent I/O error"));

        // When & Then
        assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
            .isInstanceOf(ClaimCheckStoreException.class);
      }
    }

    @Test
    void shouldFailImmediatelyWhenFileWriteFailsAndRetryDisabled() {
      // Given
      Map<String, Object> sourceTransformConfig = generateSourceConfigWithRetry(0);
      sourceTransform.configure(sourceTransformConfig);
      SourceRecord initialSourceRecord = generateSourceRecord();

      AtomicInteger writeAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // All attempts fail
        mockedFiles
            .when(() -> Files.write(any(Path.class), any(byte[].class)))
            .thenAnswer(
                invocation -> {
                  writeAttemptCount.incrementAndGet();
                  throw new IOException("I/O error");
                });

        // When & Then
        assertThatThrownBy(() -> sourceTransform.apply(initialSourceRecord))
            .isInstanceOf(ClaimCheckStoreException.class);
        // Should attempt only once without retry
        assertThat(writeAttemptCount.get()).isEqualTo(1);
      }
    }
  }

  @Nested
  class FileSystemSinkRetryIntegrationTest {

    @Test
    void shouldSucceedWithRetriesWhenFileReadFailsTemporarily() {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithRetry(3);
      sinkTransform.configure(sinkTransformConfig);
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      AtomicInteger readAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // First attempt fails, second attempt succeeds
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

        SourceRecord initialSourceRecord = generateSourceRecord();
        validateRestoredSinkRecord(restoredSinkRecord, initialSourceRecord);
      }
    }

    @Test
    void shouldFailWhenFileReadFailsPersistently() {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithRetry(2);
      sinkTransform.configure(sinkTransformConfig);
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // All attempts fail
        mockedFiles
            .when(() -> Files.readAllBytes(any(Path.class)))
            .thenThrow(new IOException("Persistent I/O error"));

        // When & Then
        assertThatExceptionOfType(ClaimCheckRetrieveException.class)
            .isThrownBy(() -> sinkTransform.apply(initialSinkRecord))
            .withMessageStartingWith("Failed to read claim check file:");
      }
    }

    @Test
    void shouldFailImmediatelyWhenFileReadFailsAndRetryDisabled() {
      // Given
      Map<String, Object> sinkTransformConfig = generateSinkConfigWithRetry(0);
      sinkTransform.configure(sinkTransformConfig);
      SinkRecord initialSinkRecord = storeDataAndCreateSinkRecord();

      AtomicInteger readAttemptCount = new AtomicInteger(0);
      try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        // All attempts fail
        mockedFiles
            .when(() -> Files.readAllBytes(any(Path.class)))
            .thenAnswer(
                invocation -> {
                  readAttemptCount.incrementAndGet();
                  throw new IOException("I/O error");
                });

        // When & Then
        assertThatExceptionOfType(ClaimCheckRetrieveException.class)
            .isThrownBy(() -> sinkTransform.apply(initialSinkRecord))
            .withMessageStartingWith("Failed to read claim check file:");
        // Should attempt only once without retry
        assertThat(readAttemptCount.get()).isEqualTo(1);
      }
    }

    private SinkRecord storeDataAndCreateSinkRecord() {
      Map<String, Object> sourceConfig = new HashMap<>();
      sourceConfig.put(
          ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG,
          ClaimCheckStorageType.FILESYSTEM.type());
      sourceConfig.put(ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, 1);
      sourceConfig.put(FileSystemStorageConfig.PATH_CONFIG, TEMP_DIR_PATH.toString());
      sourceTransform.configure(sourceConfig);

      SourceRecord initialSourceRecord = generateSourceRecord();
      SourceRecord transformedSourceRecord = sourceTransform.apply(initialSourceRecord);

      Header transformedSourceHeader =
          transformedSourceRecord.headers().lastWithName(ClaimCheckHeader.HEADER_KEY);

      return generateSinkRecord(transformedSourceRecord, transformedSourceHeader);
    }
  }

  private Map<String, Object> generateSourceConfigWithRetry(int retryMax) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG,
        ClaimCheckStorageType.FILESYSTEM.type());
    config.put(ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, 1);
    config.put(FileSystemStorageConfig.PATH_CONFIG, TEMP_DIR_PATH.toString());
    config.put(FileSystemStorageConfig.RETRY_MAX_CONFIG, retryMax);
    config.put(FileSystemStorageConfig.RETRY_BACKOFF_MS_CONFIG, 5L);
    config.put(FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG, 10L);
    return config;
  }

  private Map<String, Object> generateSinkConfigWithRetry(int retryMax) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        ClaimCheckSinkTransformConfig.STORAGE_TYPE_CONFIG, ClaimCheckStorageType.FILESYSTEM.type());
    config.put(FileSystemStorageConfig.PATH_CONFIG, TEMP_DIR_PATH.toString());
    config.put(FileSystemStorageConfig.RETRY_MAX_CONFIG, retryMax);
    config.put(FileSystemStorageConfig.RETRY_BACKOFF_MS_CONFIG, 5L);
    config.put(FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG, 10L);
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

  private void validateTransformedSourceRecord(
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

    // Validate actual data
    ClaimCheckMetadata claimCheckMetadata = ClaimCheckHeader.fromHeader(transformedSourceHeader);
    String referenceUrl = claimCheckMetadata.referenceUrl();
    int originalSizeBytes = claimCheckMetadata.originalSizeBytes();
    long uploadedAt = claimCheckMetadata.uploadedAt();

    assertThat(referenceUrl).startsWith("file://" + TEMP_DIR_PATH.toRealPath() + "/");
    assertThat(originalSizeBytes).isGreaterThan(0);
    assertThat(uploadedAt).isGreaterThan(0);

    // Verify that actual data is stored in file system
    Path filePath = Path.of(URI.create(referenceUrl).getPath());
    assertThat(Files.exists(filePath)).isTrue();
    byte[] fileContent = Files.readAllBytes(filePath);
    assertThat(fileContent).isNotEmpty();
    assertThat(fileContent.length).isEqualTo(originalSizeBytes);
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
