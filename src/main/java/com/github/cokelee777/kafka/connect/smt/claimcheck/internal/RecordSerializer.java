package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import org.apache.kafka.connect.source.SourceRecord;

public interface RecordSerializer {

  byte[] serialize(SourceRecord record);
}
