package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordValuePlaceholder} implementation for schema-based records.
 *
 * <p>This strategy generates a placeholder value based on the record's schema, filling in default
 * values for all fields according to their types. It handles primitive types, logical types
 * (Timestamp, Date, Time, Decimal), and nested structures like Structs, Arrays, and Maps.
 */
public final class SchemaBasedRecordValuePlaceholder implements RecordValuePlaceholder {

  private static final Logger log =
      LoggerFactory.getLogger(SchemaBasedRecordValuePlaceholder.class);

  private static final Map<String, Supplier<Object>> LOGICAL_TYPE_DEFAULTS =
      Map.of(
          Timestamp.LOGICAL_NAME, () -> new Date(0),
          org.apache.kafka.connect.data.Date.LOGICAL_NAME, () -> new Date(0),
          Time.LOGICAL_NAME, () -> new Date(0),
          Decimal.LOGICAL_NAME, () -> BigDecimal.ZERO);

  @Override
  public boolean supports(SourceRecord record) {
    return record.valueSchema() != null;
  }

  @Override
  public Object apply(SourceRecord record) {
    log.debug(
        "Creating default value for schema based record from topic: {}, schema: {}",
        record.topic(),
        record.valueSchema());
    return resolveDefaultValue(record.valueSchema());
  }

  private Object resolveDefaultValue(Schema schema) {
    if (schema.defaultValue() != null) {
      return schema.defaultValue();
    }

    if (schema.isOptional()) {
      return null;
    }

    if (schema.name() != null && LOGICAL_TYPE_DEFAULTS.containsKey(schema.name())) {
      return LOGICAL_TYPE_DEFAULTS.get(schema.name()).get();
    }

    return switch (schema.type()) {
      case STRUCT -> createStruct(schema);
      case ARRAY -> Collections.emptyList();
      case MAP -> Collections.emptyMap();
      case INT8 -> (byte) 0;
      case INT16 -> (short) 0;
      case INT32 -> 0;
      case INT64 -> 0L;
      case FLOAT32 -> 0.0f;
      case FLOAT64 -> 0.0;
      case BOOLEAN -> false;
      case STRING -> "";
      case BYTES -> ByteBuffer.wrap(new byte[0]);
    };
  }

  private Struct createStruct(Schema schema) {
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      struct.put(field, resolveDefaultValue(field.schema()));
    }
    return struct;
  }
}
