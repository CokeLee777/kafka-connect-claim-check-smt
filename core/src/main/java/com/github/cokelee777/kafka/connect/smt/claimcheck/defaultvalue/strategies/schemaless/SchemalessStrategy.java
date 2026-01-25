package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.strategies.schemaless;

import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.DefaultValueStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.DefaultValueStrategyType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemalessStrategy implements DefaultValueStrategy {

  private static final Logger log = LoggerFactory.getLogger(SchemalessStrategy.class);

  @Override
  public String getStrategyType() {
    return DefaultValueStrategyType.SCHEMALESS.type();
  }

  @Override
  public Schema.Type getSupportedSchemaType() {
    return null;
  }

  @Override
  public boolean canHandle(SourceRecord record) {
    return record.valueSchema() == null;
  }

  @Override
  public Object createDefaultValue(SourceRecord record) {
    if (!canHandle(record)) {
      throw new IllegalArgumentException(
          "Cannot handle record with non-null schema. Expected schemaless record.");
    }

    log.debug("Creating null value for schemaless record from topic: {}", record.topic());
    return null;
  }
}
