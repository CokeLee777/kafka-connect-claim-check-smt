package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

/** Field name constants for the claim check header JSON. */
public final class ClaimCheckHeaderFields {

  private ClaimCheckHeaderFields() {}

  /** The external storage reference URL where the offloaded payload is stored. */
  public static final String REFERENCE_URL = "reference_url";

  /** The original payload size in bytes before offloading. */
  public static final String ORIGINAL_SIZE_BYTES = "original_size_bytes";

  /** The epoch milliseconds timestamp at which the payload was uploaded. */
  public static final String UPLOADED_AT = "uploaded_at";
}
