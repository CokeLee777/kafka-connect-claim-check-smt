package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import java.time.Instant;
import org.apache.kafka.connect.data.Struct;

/**
 * Represents a "claim check" reference to a payload stored in an external system.
 *
 * <p>This object contains the location of the stored data (URL), its original size, and the
 * timestamp of when it was uploaded.
 */
public class ClaimCheckReference {

  private final String referenceUrl;
  private final long originalSizeBytes;
  private final long uploadedAt;

  private ClaimCheckReference(String referenceUrl, long originalSizeBytes, long uploadedAt) {
    this.referenceUrl = referenceUrl;
    this.originalSizeBytes = originalSizeBytes;
    this.uploadedAt = uploadedAt;
  }

  /**
   * Creates a new {@link ClaimCheckReference} with the current timestamp.
   *
   * @param referenceUrl The URL pointing to the stored payload.
   * @param originalSizeBytes The original size of the payload in bytes.
   * @return A new {@link ClaimCheckReference} instance.
   */
  public static ClaimCheckReference create(String referenceUrl, long originalSizeBytes) {
    if (referenceUrl == null || referenceUrl.isBlank()) {
      throw new IllegalArgumentException("referenceUrl must be non-blank");
    }
    if (originalSizeBytes < 0) {
      throw new IllegalArgumentException("originalSizeBytes must be >= 0");
    }
    return new ClaimCheckReference(referenceUrl, originalSizeBytes, Instant.now().toEpochMilli());
  }

  /**
   * Converts this reference object into a Kafka Connect {@link Struct} using the {@link
   * ClaimCheckSchema#SCHEMA}.
   *
   * @return A {@link Struct} representing the claim check.
   */
  public Struct toStruct() {
    return new Struct(ClaimCheckSchema.SCHEMA)
        .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
        .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, originalSizeBytes)
        .put(ClaimCheckSchemaFields.UPLOADED_AT, uploadedAt);
  }
}
