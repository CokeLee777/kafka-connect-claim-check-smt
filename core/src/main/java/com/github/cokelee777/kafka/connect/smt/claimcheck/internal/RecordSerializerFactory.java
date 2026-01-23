package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;

public class RecordSerializerFactory {

  private static final String DEFAULT_SERIALIZER_TYPE = RecordSerializerType.JSON.type();
  private static final Map<String, Supplier<RecordSerializer>> SERIALIZER_MAP = new HashMap<>();

  static {
    register(RecordSerializerType.JSON.type(), JsonRecordSerializer::create);
  }

  private static void register(String type, Supplier<RecordSerializer> supplier) {
    SERIALIZER_MAP.put(type.toLowerCase(Locale.ROOT), supplier);
  }

  public static RecordSerializer create() {
    return create(DEFAULT_SERIALIZER_TYPE);
  }

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
