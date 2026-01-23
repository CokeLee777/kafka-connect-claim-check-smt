package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("JsonRecordSerializer 단위 테스트")
class JsonRecordSerializerTest {

  @Nested
  @DisplayName("create 메서드 테스트")
  class CreateTest {

    @Test
    @DisplayName("Schema와 Schemaless Serializer가 모두 구성된다.")
    void schemaAndSchemalessSerializerConfig() {
      // Given & When
      JsonRecordSerializer jsonRecordSerializer = JsonRecordSerializer.create();

      // Then
      assertThat(jsonRecordSerializer).isNotNull();
      assertThat(jsonRecordSerializer.getSchemaValueConverter()).isNotNull();
      assertThat(jsonRecordSerializer.getSchemalessValueConverter()).isNotNull();
    }
  }

  @Nested
  @DisplayName("serialize 메서드 테스트")
  class SerializeTest {

    private final JsonRecordSerializer jsonRecordSerializer = JsonRecordSerializer.create();

    @Test
    @DisplayName("Schema가 존재하는 Record는 정상적으로 직렬화된다.")
    void schemaRecordSerializer() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-bucket", Schema.STRING_SCHEMA, "payload");
      String expectedJson =
          "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"payload\"}";
      byte[] expectedPayload = expectedJson.getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    @DisplayName("Schema가 존재하지 않는 byte[] Record도 정상적으로 직렬화된다.")
    void schemalessBytesRecordSerializer() {
      // Given
      SourceRecord record =
          new SourceRecord(
              null, null, "test-bucket", null, "payload".getBytes(StandardCharsets.UTF_8));
      byte[] expectedPayload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    @DisplayName("Schema가 존재하지 않는 String Record도 정상적으로 직렬화된다.")
    void schemalessStringRecordSerializer() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-bucket", null, "payload");
      byte[] expectedPayload = "payload".getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    @DisplayName("Schema가 존재하지 않는 Map Record도 정상적으로 직렬화된다.")
    void schemalessMapRecordSerializer() {
      // Given
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("id", 1);
      payload.put("name", "payload");
      SourceRecord record = new SourceRecord(null, null, "test-bucket", null, payload);
      String expectedJson = "{\"id\":1,\"name\":\"payload\"}";
      byte[] expectedPayload = expectedJson.getBytes(StandardCharsets.UTF_8);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isEqualTo(expectedPayload);
    }

    @Test
    @DisplayName("값이 null인 Record는 null이 반환된다.")
    void nullRecordReturnNull() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-bucket", null, null);

      // When
      byte[] serializedPayload = jsonRecordSerializer.serialize(record);

      // Then
      assertThat(serializedPayload).isNull();
    }
  }
}
