package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

/**
 * Contains constants for the field names used in the {@link ClaimCheckSchema}.
 *
 * <p>This prevents magic strings and provides a single source of truth for field names.
 */
public final class ClaimCheckSchemaFields {

  private ClaimCheckSchemaFields() {}

  /** The field name for the URL pointing to the externally stored payload. */
  public static final String REFERENCE_URL = "reference_url";

  /** The field name for the original size of the payload in bytes. */
  public static final String ORIGINAL_SIZE_BYTES = "original_size_bytes";

  /** The field name for the Unix timestamp (in milliseconds) when the payload was uploaded. */
  public static final String UPLOADED_AT = "uploaded_at";
}
