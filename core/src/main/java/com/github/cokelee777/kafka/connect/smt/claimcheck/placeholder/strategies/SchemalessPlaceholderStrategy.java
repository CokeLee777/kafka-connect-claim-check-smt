package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.PlaceholderStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.PlaceholderStrategyType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemalessPlaceholderStrategy implements PlaceholderStrategy {

  private static final Logger log = LoggerFactory.getLogger(SchemalessPlaceholderStrategy.class);

  @Override
  public String getStrategyType() {
    return PlaceholderStrategyType.SCHEMALESS.type();
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
  public Object apply(SourceRecord record) {
    if (!canHandle(record)) {
      throw new IllegalArgumentException(
          "Cannot handle record with non-null schema. Expected schemaless record.");
    }

    log.debug("Creating null value for schemaless record from topic: {}", record.topic());
    return null;
  }
}
