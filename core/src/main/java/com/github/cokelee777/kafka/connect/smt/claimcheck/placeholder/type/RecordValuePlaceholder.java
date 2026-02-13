package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Strategy for generating placeholder values when the original payload is offloaded to external
 * storage.
 *
 * <p>Used by the Source Transform to maintain schema compatibility while the actual data is stored
 * externally.
 */
public interface RecordValuePlaceholder {

  /**
   * Checks if this placeholder supports the given record.
   *
   * @param record the record to check
   * @return {@code true} if this placeholder supports the record
   */
  boolean supports(SourceRecord record);

  /**
   * Creates a placeholder value that preserves the original schema structure.
   *
   * @param record the source record
   * @return the generated placeholder value
   */
  Object apply(SourceRecord record);
}
