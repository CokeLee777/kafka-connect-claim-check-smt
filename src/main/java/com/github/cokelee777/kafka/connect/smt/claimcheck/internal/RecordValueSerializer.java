package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * An interface for serializing the value of a {@link SourceRecord}.
 *
 * <p>This is used to determine the size of the record's payload before deciding whether to apply
 * the claim check pattern.
 */
public interface RecordValueSerializer {

  /**
   * Serializes the value of a source record into a byte array.
   *
   * @param record The source record to serialize.
   * @return A byte array representing the serialized record value, or {@code null} if the value
   *     cannot be serialized.
   */
  byte[] serialize(SourceRecord record);
}
