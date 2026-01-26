package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

/** Value object representing claim check metadata stored in record headers. */
public class ClaimCheckValue {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String referenceUrl;
  private final long originalSizeBytes;
  private final long uploadedAt;

  private ClaimCheckValue(String referenceUrl, long originalSizeBytes, long uploadedAt) {
    this.referenceUrl = referenceUrl;
    this.originalSizeBytes = originalSizeBytes;
    this.uploadedAt = uploadedAt;
  }

  /** Returns the external storage reference URL. */
  public String getReferenceUrl() {
    return referenceUrl;
  }

  /** Returns the original payload size in bytes. */
  public long getOriginalSizeBytes() {
    return originalSizeBytes;
  }

  /**
   * Creates a new ClaimCheckValue with the current timestamp.
   *
   * @param referenceUrl the external storage reference URL
   * @param originalSizeBytes the original payload size in bytes
   * @return a new ClaimCheckValue instance
   */
  public static ClaimCheckValue create(String referenceUrl, long originalSizeBytes) {
    if (referenceUrl == null || referenceUrl.isBlank()) {
      throw new IllegalArgumentException("referenceUrl must be non-blank");
    }

    if (originalSizeBytes < 0) {
      throw new IllegalArgumentException("originalSizeBytes must be >= 0");
    }

    return new ClaimCheckValue(referenceUrl, originalSizeBytes, Instant.now().toEpochMilli());
  }

  /** Converts this value to a Kafka Connect Struct. */
  public Struct toStruct() {
    return new Struct(ClaimCheckSchema.SCHEMA)
        .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
        .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, originalSizeBytes)
        .put(ClaimCheckSchemaFields.UPLOADED_AT, uploadedAt);
  }

  /**
   * Parses a ClaimCheckValue from various input types (Struct, Map, String JSON).
   *
   * @param value the input value to parse
   * @return the parsed ClaimCheckValue
   * @throws ConnectException if the value type is unsupported
   */
  public static ClaimCheckValue from(Object value) {
    if (value instanceof Struct) {
      return from((Struct) value);
    }

    if (value instanceof Map) {
      return from((Map<?, ?>) value);
    }

    if (value instanceof String) {
      return fromJson((String) value);
    }

    throw new ConnectException("Unsupported claim check value type: " + value.getClass());
  }

  private static ClaimCheckValue from(Struct struct) {
    String referenceUrl = struct.getString(ClaimCheckSchemaFields.REFERENCE_URL);
    Long originalSizeBytes = struct.getInt64(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    Long uploadedAt = struct.getInt64(ClaimCheckSchemaFields.UPLOADED_AT);
    if (referenceUrl == null || originalSizeBytes == null || uploadedAt == null) {
      throw new ConnectException("Missing required fields in claim check Struct");
    }

    return new ClaimCheckValue(referenceUrl, originalSizeBytes, uploadedAt);
  }

  private static ClaimCheckValue from(Map<?, ?> map) {
    Object referenceUrl = map.get(ClaimCheckSchemaFields.REFERENCE_URL);
    Object originalSizeBytes = map.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    Object uploadedAt = map.get(ClaimCheckSchemaFields.UPLOADED_AT);
    if (referenceUrl == null || originalSizeBytes == null || uploadedAt == null) {
      throw new ConnectException("Missing required fields in claim check Map");
    }

    return new ClaimCheckValue(
        referenceUrl.toString(),
        Long.parseLong(originalSizeBytes.toString()),
        Long.parseLong(uploadedAt.toString()));
  }

  private static ClaimCheckValue fromJson(String value) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(value);
      return from(node);
    } catch (Exception e) {
      throw new ConnectException("Failed to parse claim check JSON", e);
    }
  }

  private static ClaimCheckValue from(JsonNode node) {
    JsonNode referenceUrlNode = node.get(ClaimCheckSchemaFields.REFERENCE_URL);
    JsonNode originalSizeBytesNode = node.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    JsonNode uploadedAtNode = node.get(ClaimCheckSchemaFields.UPLOADED_AT);
    if (referenceUrlNode == null || originalSizeBytesNode == null || uploadedAtNode == null) {
      throw new ConnectException("Missing required fields in claim check JSON");
    }

    return new ClaimCheckValue(
        referenceUrlNode.asText(), originalSizeBytesNode.asLong(), uploadedAtNode.asLong());
  }
}
