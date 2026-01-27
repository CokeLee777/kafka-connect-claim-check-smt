package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Strategy for generating placeholder values when the original payload is offloaded to external
 * storage.
 *
 * <p>Used by the Source Transform to maintain schema compatibility while the actual data is stored
 * externally.
 */
public interface PlaceholderStrategy {

  /**
   * Returns the strategy type identifier.
   *
   * @return the strategy type name
   */
  String getStrategyType();

  /**
   * Returns the schema type this strategy supports.
   *
   * @return the supported schema type, or {@code null} for schemaless records
   */
  Schema.Type getSupportedSchemaType();

  /**
   * Checks if this strategy can handle the given record.
   *
   * @param record the record to check
   * @return {@code true} if this strategy can handle the record
   */
  boolean canHandle(SourceRecord record);

  /**
   * Creates a placeholder value that preserves the original schema structure.
   *
   * @param record the source record
   * @return the generated placeholder value
   */
  Object apply(SourceRecord record);
}
