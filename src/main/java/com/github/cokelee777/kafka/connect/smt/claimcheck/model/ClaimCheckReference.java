package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import java.time.Instant;
import org.apache.kafka.connect.data.Struct;

public class ClaimCheckReference {

  private final String referenceUrl;
  private final long originalSizeBytes;
  private final long uploadedAt;

  private ClaimCheckReference(String referenceUrl, long originalSizeBytes, long uploadedAt) {
    this.referenceUrl = referenceUrl;
    this.originalSizeBytes = originalSizeBytes;
    this.uploadedAt = uploadedAt;
  }

  public static ClaimCheckReference create(String referenceUrl, long originalSizeBytes) {
    if (referenceUrl == null || referenceUrl.isBlank()) {
      throw new IllegalArgumentException("referenceUrl must be non-blank");
    }

    if (originalSizeBytes < 0) {
      throw new IllegalArgumentException("originalSizeBytes must be >= 0");
    }

    return new ClaimCheckReference(referenceUrl, originalSizeBytes, Instant.now().toEpochMilli());
  }

  public Struct toStruct() {
    return new Struct(ClaimCheckSchema.SCHEMA)
        .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
        .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, originalSizeBytes)
        .put(ClaimCheckSchemaFields.UPLOADED_AT, uploadedAt);
  }
}
