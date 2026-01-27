package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies.DebeziumStructPlaceholderStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies.GenericStructPlaceholderStrategy;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.strategies.SchemalessPlaceholderStrategy;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves the appropriate {@link PlaceholderStrategy} for a given record. */
public class PlaceholderStrategyResolver {

  private static final Logger log = LoggerFactory.getLogger(PlaceholderStrategyResolver.class);

  private static final List<PlaceholderStrategy> STRATEGIES =
      List.of(
          new DebeziumStructPlaceholderStrategy(),
          new GenericStructPlaceholderStrategy(),
          new SchemalessPlaceholderStrategy());

  private PlaceholderStrategyResolver() {}

  /**
   * Resolves the first strategy that can handle the given record.
   *
   * @param record the source record to resolve a strategy for
   * @return the matching strategy
   * @throws IllegalArgumentException if record is null
   * @throws IllegalStateException if no strategy can handle the record
   */
  public static PlaceholderStrategy resolve(SourceRecord record) {
    if (record == null) {
      throw new IllegalArgumentException("Source record cannot be null");
    }

    Schema schema = record.valueSchema();
    for (PlaceholderStrategy strategy : STRATEGIES) {
      if (strategy.canHandle(record)) {
        log.debug("Resolved strategy: {} for schema: {}", strategy.getStrategyType(), schema);
        return strategy;
      }
    }

    throw new IllegalStateException("No strategy found for schema: " + schema);
  }
}
