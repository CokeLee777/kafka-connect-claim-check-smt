package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RecordValuePlaceholderResolverTest {

  @Nested
  class ResolveTest {

    @Test
    void shouldReturnSchemalessPlaceholderWhenRecordIsSchemaless() {
      // Given
      Map<String, Object> value = new HashMap<>();
      value.put("id", 1L);
      value.put("name", "cokelee777");
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      RecordValuePlaceholder placeholder = RecordValuePlaceholderResolver.resolve(record);

      // Then
      assertThat(placeholder).isNotNull();
      assertThat(placeholder).isInstanceOf(SchemalessRecordValuePlaceholder.class);
    }

    @Test
    void shouldReturnSchemaBasedPlaceholderWhenRecordHasSchema() {
      // Given
      Schema rowSchema =
          SchemaBuilder.struct()
              .name("test.db.table.Value")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .optional()
              .build();
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("io.debezium.connector.mysql.Envelope")
              .field("before", rowSchema)
              .field("after", rowSchema)
              .field("op", Schema.STRING_SCHEMA)
              .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
              .build();
      Struct before = new Struct(rowSchema).put("id", 1L).put("name", "before cokelee777");
      Struct after = new Struct(rowSchema).put("id", 1L).put("name", "after cokelee777");
      long tsMs = System.currentTimeMillis();
      Struct envelope =
          new Struct(valueSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", tsMs);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, envelope);

      // When
      RecordValuePlaceholder placeholder = RecordValuePlaceholderResolver.resolve(record);

      // Then
      assertThat(placeholder).isNotNull();
      assertThat(placeholder).isInstanceOf(SchemaBasedRecordValuePlaceholder.class);
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldThrowExceptionWhenRecordIsNull() {
      // Given
      SourceRecord nullRecord = null;

      // When & Then
      assertThatExceptionOfType(DataException.class)
          .isThrownBy(() -> RecordValuePlaceholderResolver.resolve(nullRecord))
          .withMessage("Source record cannot be null for placeholder resolution.");
    }
  }
}
