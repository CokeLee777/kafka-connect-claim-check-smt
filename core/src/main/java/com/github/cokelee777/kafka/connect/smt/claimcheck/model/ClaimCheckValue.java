package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

/** Value object representing claim check metadata stored in record headers. */
public record ClaimCheckValue(String referenceUrl, int originalSizeBytes, long uploadedAt) {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ClaimCheckValue {
    if (referenceUrl == null || referenceUrl.isBlank()) {
      throw new DataException("referenceUrl must be non-blank");
    }

    if (originalSizeBytes < 0) {
      throw new DataException("originalSizeBytes must be >= 0");
    }

    if (uploadedAt <= 0) {
      throw new DataException("uploadedAt must be positive epoch millis");
    }
  }

  /**
   * Creates a new ClaimCheckValue with the current timestamp.
   *
   * @param referenceUrl the external storage reference URL
   * @param originalSizeBytes the original payload size in bytes
   * @return a new ClaimCheckValue instance
   */
  public static ClaimCheckValue create(String referenceUrl, int originalSizeBytes) {
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
    if (value == null) {
      throw new DataException("Claim check value cannot be null");
    }

    if (value instanceof Struct structValue) {
      return from(structValue);
    } else if (value instanceof Map<?, ?> mapValue) {
      return from(mapValue);
    } else if (value instanceof String strValue) {
      return fromJson(strValue);
    }

    throw new ConnectException("Unsupported claim check value type: " + value.getClass());
  }

  private static ClaimCheckValue from(Struct struct) {
    String referenceUrl = struct.getString(ClaimCheckSchemaFields.REFERENCE_URL);
    if (referenceUrl == null) {
      throw new DataException(
          "Missing required field '"
              + ClaimCheckSchemaFields.REFERENCE_URL
              + "' in claim check Struct");
    }

    Integer originalSizeBytes = struct.getInt32(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    if (originalSizeBytes == null) {
      throw new DataException(
          "Missing required field '"
              + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
              + "' in claim check Struct");
    }

    Long uploadedAt = struct.getInt64(ClaimCheckSchemaFields.UPLOADED_AT);
    if (uploadedAt == null) {
      throw new DataException(
          "Missing required field '"
              + ClaimCheckSchemaFields.UPLOADED_AT
              + "' in claim check Struct");
    }

    return new ClaimCheckValue(referenceUrl, originalSizeBytes, uploadedAt);
  }

  private static ClaimCheckValue from(Map<?, ?> map) {
    Object referenceUrlObj = map.get(ClaimCheckSchemaFields.REFERENCE_URL);
    Object originalSizeBytesObj = map.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    Object uploadedAtObj = map.get(ClaimCheckSchemaFields.UPLOADED_AT);

    if (referenceUrlObj == null) {
      throw new DataException(
          "Missing required field '"
              + ClaimCheckSchemaFields.REFERENCE_URL
              + "' in claim check Map");
    }

    if (!(referenceUrlObj instanceof String referenceUrl)) {
      throw new DataException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.REFERENCE_URL
              + "': expected String, got "
              + referenceUrlObj.getClass().getSimpleName());
    }

      if (originalSizeBytesObj == null) {
      throw new DataException(
          "Missing required field '"
              + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
              + "' in claim check Map");
    }
    int originalSizeBytes =
        parseInteger(originalSizeBytesObj, ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);

    if (uploadedAtObj == null) {
      throw new DataException(
          "Missing required field '" + ClaimCheckSchemaFields.UPLOADED_AT + "' in claim check Map");
    }
    long uploadedAt = parseLong(uploadedAtObj, ClaimCheckSchemaFields.UPLOADED_AT);

    return new ClaimCheckValue(referenceUrl, originalSizeBytes, uploadedAt);
  }

  private static int parseInteger(Object value, String fieldName) {
    if (value instanceof Integer intValue) {
      return intValue;
    }

    if (value instanceof Long longValue) {
      if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
        throw new DataException("Value out of Integer range for '" + fieldName + "': " + longValue);
      }
      return longValue.intValue();
    }

    if (value instanceof Short shortValue) {
      return shortValue.intValue();
    }

    throw new DataException(
        "Invalid type for '"
            + fieldName
            + "': expected Integer, got "
            + value.getClass().getSimpleName());
  }

  private static long parseLong(Object value, String fieldName) {
    if (value instanceof Long longValue) {
      return longValue;
    }

    if (value instanceof Integer intValue) {
      return intValue.longValue();
    }

    throw new DataException(
        "Invalid type for '"
            + fieldName
            + "': expected Long, got "
            + value.getClass().getSimpleName());
  }

  private static ClaimCheckValue fromJson(String value) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(value);
      return from(node);
    } catch (DataException e) {
      throw e;
    } catch (Exception e) {
      throw new DataException("Failed to parse claim check JSON", e);
    }
  }

  private static ClaimCheckValue from(JsonNode node) {
    JsonNode referenceUrlNode = node.get(ClaimCheckSchemaFields.REFERENCE_URL);
    JsonNode originalSizeBytesNode = node.get(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES);
    JsonNode uploadedAtNode = node.get(ClaimCheckSchemaFields.UPLOADED_AT);
    if (referenceUrlNode == null || originalSizeBytesNode == null || uploadedAtNode == null) {
      throw new DataException("Missing required fields in claim check JSON");
    }

    if (!referenceUrlNode.isTextual()) {
      throw new DataException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.REFERENCE_URL
              + "': expected STRING, got "
              + referenceUrlNode.getNodeType());
    }
    if (!originalSizeBytesNode.isInt()) {
      throw new DataException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES
              + "': expected INT, got "
              + originalSizeBytesNode.getNodeType());
    }
    if (!uploadedAtNode.isIntegralNumber()) {
      throw new DataException(
          "Invalid type for '"
              + ClaimCheckSchemaFields.UPLOADED_AT
              + "': expected LONG, got "
              + uploadedAtNode.getNodeType());
    }

    return new ClaimCheckValue(
        referenceUrlNode.asText(), originalSizeBytesNode.intValue(), uploadedAtNode.longValue());
  }
}
