package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

/** Value object representing claim check metadata stored in record headers. */
public class ClaimCheckValue {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String referenceUrl;
  private final int originalSizeBytes;
  private final long uploadedAt;

  private ClaimCheckValue(String referenceUrl, int originalSizeBytes, long uploadedAt) {
    this.referenceUrl = referenceUrl;
    this.originalSizeBytes = originalSizeBytes;
    this.uploadedAt = uploadedAt;
  }

  /** Returns the external storage reference URL. */
  public String getReferenceUrl() {
    return referenceUrl;
  }

  /** Returns the original payload size in bytes. */
  public int getOriginalSizeBytes() {
    return originalSizeBytes;
  }

  /**
   * Creates a new ClaimCheckValue with the current timestamp.
   *
   * @param referenceUrl the external storage reference URL
   * @param originalSizeBytes the original payload size in bytes
   * @return a new ClaimCheckValue instance
   */
  public static ClaimCheckValue create(String referenceUrl, int originalSizeBytes) {
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
    try {
      String referenceUrl = struct.getString(ClaimCheckSchemaFields.REFERENCE_URL);
      if (referenceUrl == null) {
        throw new ConnectException(
            "Missing required field '"
                + ClaimCheckSchemaFields.REFERENCE_URL
                + "' in claim check Struct");
      }

      Integer originalSizeBytes = struct.getInt32(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
      if (originalSizeBytes == null) {
        throw new ConnectException(
            "Missing required field '"
                + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
                + "' in claim check Struct");
      }

      Long uploadedAt = struct.getInt64(ClaimCheckSchemaFields.UPLOADED_AT);
      if (uploadedAt == null) {
        throw new ConnectException(
            "Missing required field '"
                + ClaimCheckSchemaFields.UPLOADED_AT
                + "' in claim check Struct");
      }

      return new ClaimCheckValue(referenceUrl, originalSizeBytes, uploadedAt);
    } catch (DataException e) {
      throw new ConnectException("Invalid field type in claim check Struct: " + e.getMessage(), e);
    }
  }

  private static ClaimCheckValue from(Map<?, ?> map) {
    Object referenceUrlObj = map.get(ClaimCheckSchemaFields.REFERENCE_URL);
    Object originalSizeBytesObj = map.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    Object uploadedAtObj = map.get(ClaimCheckSchemaFields.UPLOADED_AT);

    if (referenceUrlObj == null) {
      throw new ConnectException(
          "Missing required field '"
              + ClaimCheckSchemaFields.REFERENCE_URL
              + "' in claim check Map");
    }

    if (!(referenceUrlObj instanceof String)) {
      throw new ConnectException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
              + "': expected String, got "
              + referenceUrlObj.getClass().getSimpleName());
    }
    String referenceUrl = (String) referenceUrlObj;

    if (originalSizeBytesObj == null) {
      throw new ConnectException(
          "Missing required field '"
              + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
              + "' in claim check Map");
    }
    int originalSizeBytes =
        parseInteger(originalSizeBytesObj, ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);

    if (uploadedAtObj == null) {
      throw new ConnectException(
          "Missing required field '" + ClaimCheckSchemaFields.UPLOADED_AT + "' in claim check Map");
    }
    long uploadedAt = parseLong(uploadedAtObj, ClaimCheckSchemaFields.UPLOADED_AT);

    return new ClaimCheckValue(referenceUrl, originalSizeBytes, uploadedAt);
  }

  private static int parseInteger(Object value, String fieldName) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (NumberFormatException e) {
      throw new ConnectException(
          "Invalid type for '"
              + fieldName
              + "': expected Integer, got "
              + value.getClass().getSimpleName(),
          e);
    }
  }

  private static long parseLong(Object value, String fieldName) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    try {
      return Long.parseLong(value.toString());
    } catch (NumberFormatException e) {
      throw new ConnectException(
          "Invalid type for '"
              + fieldName
              + "': expected Long, got "
              + value.getClass().getSimpleName(),
          e);
    }
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

    if (!referenceUrlNode.isTextual()) {
      throw new ConnectException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.REFERENCE_URL
              + "': expected STRING, got "
              + referenceUrlNode.getNodeType());
    }
    if (!originalSizeBytesNode.isInt()) {
      throw new ConnectException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
              + "': expected INT, got "
              + originalSizeBytesNode.getNodeType());
    }
    if (!uploadedAtNode.isIntegralNumber()) {
      throw new ConnectException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.UPLOADED_AT
              + "': expected LONG, got "
              + uploadedAtNode.getNodeType());
    }

    return new ClaimCheckValue(
        referenceUrlNode.asText(), originalSizeBytesNode.intValue(), uploadedAtNode.longValue());
  }
}
