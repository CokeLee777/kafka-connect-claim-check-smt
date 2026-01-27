package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.PlaceholderStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.PlaceholderStrategyType;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumStructPlaceholderStrategy implements PlaceholderStrategy {

  private static final Logger log =
      LoggerFactory.getLogger(DebeziumStructPlaceholderStrategy.class);

  private static final Set<String> DEBEZIUM_METADATA_FIELDS = Set.of("source", "op");
  private static final Set<String> DEBEZIUM_DATA_FIELDS = Set.of("before", "after");

  @Override
  public String getStrategyType() {
    return PlaceholderStrategyType.DEBEZIUM_STRUCT.type();
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
        && value instanceof Struct
        && isDebeziumSchema(schema);
  }

  private boolean isDebeziumSchema(Schema schema) {
    String schemaName = schema.name();
    if (schemaName != null) {
      String lower = schemaName.toLowerCase();
      if (lower.contains("envelope")
          || lower.contains("debezium")
          || lower.startsWith("io.debezium")) {
        return true;
      }
    }

    Set<String> fieldNames = schema.fields().stream().map(Field::name).collect(Collectors.toSet());
    return fieldNames.containsAll(DEBEZIUM_METADATA_FIELDS);
  }

  @Override
  public Object apply(SourceRecord record) {
    if (!canHandle(record)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot handle record. Expected Debezium STRUCT schema. "
                  + "Got schema: %s, value type: %s",
              record.valueSchema() != null ? record.valueSchema().name() : "null",
              record.value() != null ? record.value().getClass().getSimpleName() : "null"));
    }

    Schema schema = record.valueSchema();
    Struct originalValue = (Struct) record.value();
    return createDebeziumDefaultStruct(schema, originalValue);
  }

  private Struct createDebeziumDefaultStruct(Schema schema, Struct originalValue) {
    Struct newStruct = new Struct(schema);

    Set<String> fieldNames = schema.fields().stream().map(Field::name).collect(Collectors.toSet());
    Set<String> actualDataFields =
        DEBEZIUM_DATA_FIELDS.stream().filter(fieldNames::contains).collect(Collectors.toSet());

    log.debug("Creating Debezium default struct. Data fields to nullify: {}", actualDataFields);

    for (Field field : schema.fields()) {
      String fieldName = field.name();
      if (actualDataFields.contains(fieldName)) {
        newStruct.put(field, null);
        log.debug("Data field '{}' set to null", fieldName);
      } else {
        newStruct.put(field, originalValue.get(field));
        log.debug("Metadata field '{}' preserved from original", fieldName);
      }
    }

    return newStruct;
  }
}
