package com.github.cokelee777.kafka.connect.smt.claimcheck.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;

/**
 * Immutable value object representing claim check metadata.
 *
 * <p>Captures the external storage reference and upload context for a payload that has been
 * offloaded from a Kafka record. Instances are created via {@link #create(String, int)} and
 * serialized to/from JSON for storage as a Kafka Connect header.
 */
public record ClaimCheckMetadata(String referenceUrl, int originalSizeBytes, long uploadedAt) {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Compact constructor that validates all fields.
   *
   * @throws DataException if any field fails validation
   */
  public ClaimCheckMetadata {
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
   * Creates a new {@link ClaimCheckMetadata} with the current timestamp.
   *
   * @param referenceUrl the external storage reference URL where the payload is stored
   * @param originalSizeBytes the original payload size in bytes
   * @return a new {@link ClaimCheckMetadata} instance
   */
  public static ClaimCheckMetadata create(String referenceUrl, int originalSizeBytes) {
    return new ClaimCheckMetadata(referenceUrl, originalSizeBytes, Instant.now().toEpochMilli());
  }

  /**
   * Serializes this metadata to a JSON string.
   *
   * @return a JSON string representation of this metadata
   * @throws DataException if serialization fails
   */
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(
          Map.of(
              ClaimCheckHeaderFields.REFERENCE_URL, referenceUrl,
              ClaimCheckHeaderFields.ORIGINAL_SIZE_BYTES, originalSizeBytes,
              ClaimCheckHeaderFields.UPLOADED_AT, uploadedAt));
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize ClaimCheckMetadata to JSON", e);
    }
  }

  /**
   * Parses a {@link ClaimCheckMetadata} from a JSON string.
   *
   * @param json the JSON string to parse
   * @return the parsed {@link ClaimCheckMetadata}
   * @throws DataException if the JSON is malformed, missing required fields, or contains invalid
   *     field types
   */
  public static ClaimCheckMetadata fromJson(String json) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(json);

      JsonNode referenceUrlNode = node.get(ClaimCheckHeaderFields.REFERENCE_URL);
      JsonNode originalSizeBytesNode = node.get(ClaimCheckHeaderFields.ORIGINAL_SIZE_BYTES);
      JsonNode uploadedAtNode = node.get(ClaimCheckHeaderFields.UPLOADED_AT);

      if (referenceUrlNode == null || originalSizeBytesNode == null || uploadedAtNode == null) {
        throw new DataException("Missing required fields in claim check JSON");
      }

      if (!referenceUrlNode.isTextual()) {
        throw new DataException(
            "Invalid type for '"
                + ClaimCheckHeaderFields.REFERENCE_URL
                + "': expected STRING, got "
                + referenceUrlNode.getNodeType());
      }

      if (!originalSizeBytesNode.isInt()) {
        throw new DataException(
            "Invalid type for '"
                + ClaimCheckHeaderFields.ORIGINAL_SIZE_BYTES
                + "': expected INT, got "
                + originalSizeBytesNode.getNodeType());
      }

      if (!uploadedAtNode.isLong()) {
        throw new DataException(
            "Invalid type for '"
                + ClaimCheckHeaderFields.UPLOADED_AT
                + "': expected LONG, got "
                + uploadedAtNode.getNodeType());
      }

      return new ClaimCheckMetadata(
          referenceUrlNode.asText(), originalSizeBytesNode.intValue(), uploadedAtNode.longValue());
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to parse claim check JSON", e);
    }
  }

  /**
   * Parses a {@link ClaimCheckMetadata} from a {@link Map}.
   *
   * <p>This handles the case where Kafka Connect deserializes a JSON string header value into a
   * {@link Map} instead of a {@link String}, depending on the converter configuration.
   *
   * @param map the map to parse
   * @return the parsed {@link ClaimCheckMetadata}
   * @throws DataException if required fields are missing or contain invalid types
   */
  public static ClaimCheckMetadata fromMap(Map<?, ?> map) {
    Object referenceUrlObj = map.get(ClaimCheckHeaderFields.REFERENCE_URL);
    Object originalSizeBytesObj = map.get(ClaimCheckHeaderFields.ORIGINAL_SIZE_BYTES);
    Object uploadedAtObj = map.get(ClaimCheckHeaderFields.UPLOADED_AT);

    if (referenceUrlObj == null || originalSizeBytesObj == null || uploadedAtObj == null) {
      throw new DataException("Missing required fields in claim check Map");
    }

    if (!(referenceUrlObj instanceof String referenceUrl)) {
      throw new DataException(
          "Invalid type for '"
              + ClaimCheckHeaderFields.REFERENCE_URL
              + "': expected String, got "
              + referenceUrlObj.getClass().getSimpleName());
    }

    int originalSizeBytes = parseInteger(originalSizeBytesObj);
    long uploadedAt = parseLong(uploadedAtObj);

    return new ClaimCheckMetadata(referenceUrl, originalSizeBytes, uploadedAt);
  }

  private static int parseInteger(Object value) {
    if (value instanceof Short shortValue) {
      return shortValue.intValue();
    }

    if (value instanceof Integer intValue) {
      return intValue;
    }

    if (value instanceof Long longValue) {
      if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
        throw new DataException("Value out of Integer range for original_size_bytes: " + longValue);
      }
      return longValue.intValue();
    }

    throw new DataException(
        "Invalid type for '"
            + ClaimCheckHeaderFields.ORIGINAL_SIZE_BYTES
            + "': expected Integer, got "
            + value.getClass().getSimpleName());
  }

  private static long parseLong(Object value) {
    if (value instanceof Short shortValue) {
      return shortValue.longValue();
    }

    if (value instanceof Integer intValue) {
      return intValue.longValue();
    }

    if (value instanceof Long longValue) {
      return longValue;
    }

    throw new DataException(
        "Invalid type for '"
            + ClaimCheckHeaderFields.UPLOADED_AT
            + "': expected Long, got "
            + value.getClass().getSimpleName());
  }
}
