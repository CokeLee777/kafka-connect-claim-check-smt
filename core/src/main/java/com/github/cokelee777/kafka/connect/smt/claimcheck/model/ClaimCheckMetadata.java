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
      if (!uploadedAtNode.isIntegralNumber()) {
        throw new DataException(
            "Invalid type for '"
                + ClaimCheckHeaderFields.UPLOADED_AT
                + "': expected LONG, got "
                + uploadedAtNode.getNodeType());
      }

      return new ClaimCheckMetadata(
          referenceUrlNode.asText(), originalSizeBytesNode.intValue(), uploadedAtNode.longValue());

    } catch (DataException e) {
      throw e;
    } catch (Exception e) {
      throw new DataException("Failed to parse claim check JSON", e);
    }
  }
}
