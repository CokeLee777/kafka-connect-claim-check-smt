package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.PlaceholderStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.PlaceholderStrategyType;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericStructPlaceholderStrategy implements PlaceholderStrategy {

  private static final Logger log = LoggerFactory.getLogger(GenericStructPlaceholderStrategy.class);

  @Override
  public String getStrategyType() {
    return PlaceholderStrategyType.GENERIC_STRUCT.type();
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
  public Object apply(SourceRecord record) {
    if (!canHandle(record)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot handle record. Expected STRUCT schema with Struct value. "
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
    if (schema.defaultValue() != null) {
      return schema.defaultValue();
    }

    if (isComplexType(schema)) {
      return createDefaultForComplexType(schema);
    }

    if (schema.isOptional()) {
      return null;
    }

    switch (schema.type()) {
      case STRING:
        return "";
      case INT8:
        return (byte) 0;
      case INT16:
        return (short) 0;
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
      default:
        return null;
    }
  }

  private boolean isComplexType(Schema schema) {
    Schema.Type type = schema.type();
    return type == Schema.Type.STRUCT || type == Schema.Type.ARRAY || type == Schema.Type.MAP;
  }

  private Object createDefaultForComplexType(Schema schema) {
    switch (schema.type()) {
      case STRUCT:
        return createNestedDefaultStruct(schema);
      case ARRAY:
        return Collections.emptyList();
      case MAP:
        return Collections.emptyMap();
      default:
        throw new IllegalArgumentException("Not a complex type: " + schema.type());
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
