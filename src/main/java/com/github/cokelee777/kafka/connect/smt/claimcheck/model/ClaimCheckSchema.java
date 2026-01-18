package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Defines the Kafka Connect {@link Schema} for a claim check.
 *
 * <p>This class provides a constant, shared schema for the structure that holds the reference to an
 * externally stored payload.
 */
public final class ClaimCheckSchema {

  public static final String NAME = ClaimCheckSchema.class.getName();

  /** The official Kafka Connect {@link Schema} for a claim check struct. */
  public static final Schema SCHEMA =
      SchemaBuilder.struct()
          .name(NAME)
          .field(ClaimCheckSchemaFields.REFERENCE_URL, Schema.STRING_SCHEMA)
          .field(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, Schema.INT64_SCHEMA)
          .field(ClaimCheckSchemaFields.UPLOADED_AT, Schema.INT64_SCHEMA)
          .build();

  private ClaimCheckSchema() {}
}
