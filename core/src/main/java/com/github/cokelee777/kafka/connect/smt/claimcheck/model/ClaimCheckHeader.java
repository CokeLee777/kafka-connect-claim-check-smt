package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;

/**
 * Utility for reading and writing {@link ClaimCheckMetadata} as a String-schema Kafka Connect
 * header.
 *
 * <p>Metadata is serialized as a JSON string with {@link Schema#STRING_SCHEMA}, consistent with the
 * behavior of Kafka Connect's built-in {@code InsertHeader} SMT.
 */
public final class ClaimCheckHeader {

  public static final String HEADER_KEY = "smt-toolkit-claim-check-reference";

  private ClaimCheckHeader() {}

  /**
   * Converts a {@link ClaimCheckMetadata} to a Kafka Connect {@link SchemaAndValue} suitable for
   * use as a record header value.
   *
   * @param metadata the metadata to convert
   * @return a {@link SchemaAndValue} with {@link Schema#STRING_SCHEMA} and a JSON string value
   * @throws DataException if serialization fails
   */
  public static SchemaAndValue toHeader(ClaimCheckMetadata metadata) {
    return new SchemaAndValue(Schema.STRING_SCHEMA, metadata.toJson());
  }

  /**
   * Parses a {@link ClaimCheckMetadata} from a Kafka Connect {@link Header}.
   *
   * @param header the header to parse
   * @return the parsed {@link ClaimCheckMetadata}
   * @throws DataException if the header is null, its value is null, its value is not a String, or
   *     the JSON is invalid
   */
  public static ClaimCheckMetadata fromHeader(Header header) {
    if (header == null || header.value() == null) {
      throw new DataException("Header or header value is null");
    }
    if (!(header.value() instanceof String json)) {
      throw new DataException(
          "Expected String header value, got: " + header.value().getClass().getSimpleName());
    }
    return ClaimCheckMetadata.fromJson(json);
  }
}
