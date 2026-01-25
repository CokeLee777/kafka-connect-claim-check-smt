package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue;

import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.strategies.schemaless.SchemalessStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.strategies.struct.DebeziumStructStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.strategies.struct.FlatStructStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultValueStrategySelector {

  private static final Logger log = LoggerFactory.getLogger(DefaultValueStrategySelector.class);

  private final List<DefaultValueStrategy> strategies;

  public DefaultValueStrategySelector() {
    this(createDefaultStrategies());
  }

  public DefaultValueStrategySelector(List<DefaultValueStrategy> strategies) {
    if (strategies == null || strategies.isEmpty()) {
      throw new IllegalArgumentException("At least one strategy must be provided");
    }
    this.strategies = Collections.unmodifiableList(new ArrayList<>(strategies));

    log.info("Initialized DefaultValueStrategySelector with {} strategies", strategies.size());
  }

  private static List<DefaultValueStrategy> createDefaultStrategies() {
    List<DefaultValueStrategy> strategies = new ArrayList<>();
    strategies.add(new DebeziumStructStrategy());
    strategies.add(new FlatStructStrategy());
    strategies.add(new SchemalessStrategy());

    return strategies;
  }

  public DefaultValueStrategy selectStrategy(SourceRecord record) {
    if (record == null) {
      throw new IllegalArgumentException("Source record cannot be null");
    }

    Schema schema = record.valueSchema();
    for (DefaultValueStrategy strategy : strategies) {
      if (strategy.canHandle(record)) {
        log.debug("Selected strategy type: {} for schema: {}", strategy.getStrategyType(), schema);
        return strategy;
      }
    }

    throw new IllegalStateException(
        "No strategy found for schema: " + schema.name() + ", schema type: " + schema.type());
  }
}
