package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.strategies.struct;

import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.DefaultValueStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.DefaultValueStrategyType;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatStructStrategy implements DefaultValueStrategy {

  private static final Logger log = LoggerFactory.getLogger(FlatStructStrategy.class);

  @Override
  public String getStrategyType() {
    return DefaultValueStrategyType.FLAT_STRUCT.type();
  }

  @Override
  public Schema.Type getSupportedSchemaType() {
    return Schema.Type.STRUCT;
  }

  @Override
  public boolean canHandle(SourceRecord record) {
    Schema schema = record.valueSchema();
    Object value = record.value();
    return schema != null
        && schema.type() == this.getSupportedSchemaType()
        && value instanceof Struct;
  }

  @Override
  public Object createDefaultValue(SourceRecord record) {
    if (!canHandle(record)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot handle record. Expected non-Debezium STRUCT schema. "
                  + "Got schema: %s, value type: %s",
              record.valueSchema() != null ? record.valueSchema().name() : "null",
              record.value() != null ? record.value().getClass().getSimpleName() : "null"));
    }

    Schema schema = record.valueSchema();
    return createFlatDefaultStruct(schema);
  }

  private Struct createFlatDefaultStruct(Schema schema) {
    Struct newStruct = new Struct(schema);

    log.debug("Creating flat default struct for schema: {}", schema.name());

    for (Field field : schema.fields()) {
      Object defaultValue = getDefaultValueForField(field);
      newStruct.put(field, defaultValue);
      log.debug("Field '{}' set to default value: {}", field.name(), defaultValue);
    }

    return newStruct;
  }

  private Object getDefaultValueForField(Field field) {
    Schema schema = field.schema();
    if (schema.isOptional()) {
      return null;
    }

    if (schema.defaultValue() != null) {
      return schema.defaultValue();
    }

    switch (schema.type()) {
      case STRING:
        return "";
      case INT32:
        return 0;
      case INT64:
        return 0L;
      case FLOAT32:
        return 0.0f;
      case FLOAT64:
        return 0.0;
      case BOOLEAN:
        return false;
      case BYTES:
        return ByteBuffer.wrap(new byte[0]);
      case ARRAY:
        return Collections.emptyList();
      case MAP:
        return Collections.emptyMap();
      case STRUCT:
        return createNestedDefaultStruct(schema);
      default:
        return null;
    }
  }

  private Struct createNestedDefaultStruct(Schema schema) {
    Struct nestedStruct = new Struct(schema);
    for (Field field : schema.fields()) {
      nestedStruct.put(field, getDefaultValueForField(field));
    }
    return nestedStruct;
  }
}
