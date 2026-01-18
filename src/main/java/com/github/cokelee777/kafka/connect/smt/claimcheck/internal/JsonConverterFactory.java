package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;

/**
 * A factory for creating pre-configured {@link JsonConverter} instances.
 *
 * <p>This class provides a convenient way to get a {@code JsonConverter} set up for serializing a
 * record's value without including schema information in the output.
 */
public class JsonConverterFactory {

  /**
   * Creates and configures a {@link JsonConverter} for serializing record values.
   *
   * <p>The converter is configured with {@code schemas.enable=false} to produce schemaless JSON.
   *
   * @return A configured {@link JsonConverter} instance.
   */
  public static JsonConverter createValueConverter() {
    JsonConverter converter = new JsonConverter();
    Map<String, Object> configs = Map.of("schemas.enable", false, "converter.type", "value");
    converter.configure(configs);
    return converter;
  }

  private JsonConverterFactory() {}
}
