package com.github.cokelee777.kafka.connect.smt.claimcheck;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.*;

/**
 * Creates default record values that conform to Kafka Connect schemas.
 *
 * <p>Used to create placeholder record values for claim check pattern, replacing large payloads
 * while maintaining schema compatibility.
 */
public class RecordValueDefaults {

  private static final Map<String, Supplier<Object>> LOGICAL_TYPE_DEFAULTS =
      Map.of(
          Timestamp.LOGICAL_NAME, () -> new Date(0),
          org.apache.kafka.connect.data.Date.LOGICAL_NAME, () -> new Date(0),
          Time.LOGICAL_NAME, () -> new Date(0),
          Decimal.LOGICAL_NAME, () -> BigDecimal.ZERO);

  private RecordValueDefaults() {}

  /**
   * Creates a default value for schemaless records.
   *
   * @return null (schemaless records use null as placeholder)
   */
  public static Object forSchemaless() {
    return null;
  }

  /**
   * Creates a default value for the given schema.
   *
   * @param schema the schema to create a default value for
   * @return the default value appropriate for the schema type
   */
  public static Object forSchema(Schema schema) {
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

  private static Struct createStruct(Schema schema) {
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      struct.put(field, forSchema(field.schema()));
    }
    return struct;
  }
}
