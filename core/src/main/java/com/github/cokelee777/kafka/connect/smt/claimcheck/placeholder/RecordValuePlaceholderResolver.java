package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder;

import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type.DebeziumStructRecordValuePlaceholder;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type.GenericStructRecordValuePlaceholder;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type.RecordValuePlaceholder;
import com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type.SchemalessRecordValuePlaceholder;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves the appropriate {@link RecordValuePlaceholder} for a given record. */
public class RecordValuePlaceholderResolver {

  private static final Logger log = LoggerFactory.getLogger(RecordValuePlaceholderResolver.class);

  private static final List<RecordValuePlaceholder> STRATEGIES =
      List.of(
          new DebeziumStructRecordValuePlaceholder(),
          new GenericStructRecordValuePlaceholder(),
          new SchemalessRecordValuePlaceholder());

  private RecordValuePlaceholderResolver() {}

  /**
   * Resolves the first strategy that can handle the given record.
   *
   * @param record the source record to resolve a strategy for
   * @return the matching strategy
   * @throws DataException if record is null
   * @throws DataException if no strategy can handle the record
   */
  public static RecordValuePlaceholder resolve(SourceRecord record) {
    if (record == null) {
      throw new DataException("Source record cannot be null for placeholder resolution.");
    }

    Schema schema = record.valueSchema();
    for (RecordValuePlaceholder strategy : STRATEGIES) {
      if (strategy.canHandle(record)) {
        log.debug("Resolved strategy: {} for schema: {}", strategy.getPlaceholderType(), schema);
        return strategy;
      }
    }

    throw new DataException("No placeholder resolution strategy found for schema: " + schema.type().name() + ". This usually indicates an unsupported record schema for claim check processing.");
  }
}
