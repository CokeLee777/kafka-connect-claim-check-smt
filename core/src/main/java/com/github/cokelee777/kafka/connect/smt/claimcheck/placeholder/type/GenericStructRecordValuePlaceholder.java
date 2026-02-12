package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.RecordValuePlaceholderType;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordValuePlaceholder} implementation for generic {@link Struct} records.
 *
 * <p>This strategy generates a placeholder {@link Struct} by replacing all fields
 * with their default values (if defined in the schema), or a sensible default for the field type.
 * For complex types (Struct, Array, Map), it recursively applies default values.
 * This is used for {@link Struct} records that are not specifically identified as Debezium records.
 */
public final class GenericStructRecordValuePlaceholder implements RecordValuePlaceholder {

  private static final Logger log =
      LoggerFactory.getLogger(GenericStructRecordValuePlaceholder.class);

  @Override
  public String getPlaceholderType() {
    return RecordValuePlaceholderType.GENERIC_STRUCT.type();
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
      throw new DataException(
          String.format(
              "Cannot handle record. Expected STRUCT schema with Struct value for placeholder processing. "
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

    return switch (schema.type()) {
      case STRING -> "";
      case INT8 -> (byte) 0;
      case INT16 -> (short) 0;
      case INT32 -> 0;
      case INT64 -> 0L;
      case FLOAT32 -> 0.0f;
      case FLOAT64 -> 0.0;
      case BOOLEAN -> false;
      case BYTES -> ByteBuffer.wrap(new byte[0]);
      default -> null;
    };
  }

  private boolean isComplexType(Schema schema) {
    Schema.Type type = schema.type();
    return type == Schema.Type.STRUCT || type == Schema.Type.ARRAY || type == Schema.Type.MAP;
  }

  private Object createDefaultForComplexType(Schema schema) {
    return switch (schema.type()) {
      case STRUCT -> createNestedDefaultStruct(schema);
      case ARRAY -> Collections.emptyList();
      case MAP -> Collections.emptyMap();
      default -> throw new DataException("Unsupported schema type for struct field processing: " + schema.type() + ". Expected a complex type like STRUCT or ARRAY for recursive processing.");
    };
  }

  private Struct createNestedDefaultStruct(Schema schema) {
    Struct nestedStruct = new Struct(schema);
    for (Field field : schema.fields()) {
      nestedStruct.put(field, getDefaultValueForField(field));
    }
    return nestedStruct;
  }
}
