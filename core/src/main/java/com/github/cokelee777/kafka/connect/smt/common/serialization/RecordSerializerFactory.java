package com.github.cokelee777.kafka.connect.smt.common.serialization;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;

/** Factory for creating {@link RecordSerializer} instances. */
public class RecordSerializerFactory {

  private static final String DEFAULT_SERIALIZER_TYPE = RecordSerializerType.JSON.type();
  private static final Map<String, Supplier<RecordSerializer>> SERIALIZER_MAP = new HashMap<>();

  static {
    register(RecordSerializerType.JSON.type(), JsonRecordSerializer::create);
  }

  private static void register(String type, Supplier<RecordSerializer> supplier) {
    SERIALIZER_MAP.put(type.toLowerCase(Locale.ROOT), supplier);
  }

  /**
   * Creates a serializer with the default type (JSON).
   *
   * @return a new RecordSerializer instance
   */
  public static RecordSerializer create() {
    return create(DEFAULT_SERIALIZER_TYPE);
  }

  /**
   * Creates a serializer of the specified type.
   *
   * @param type the serializer type
   * @return a new RecordSerializer instance
   * @throws ConfigException if the type is unsupported
   */
  public static RecordSerializer create(String type) {
    if (type == null || type.isBlank()) {
      return create();
    }

    Supplier<RecordSerializer> supplier = SERIALIZER_MAP.get(type.toLowerCase(Locale.ROOT));
    if (supplier == null) {
      throw new ConfigException("Unsupported serializer type: " + type);
    }

    return supplier.get();
  }

  private RecordSerializerFactory() {}
}
