package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors;

import org.apache.kafka.connect.errors.ConnectException;

public abstract class ClaimCheckStorageException extends ConnectException {

  protected ClaimCheckStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
