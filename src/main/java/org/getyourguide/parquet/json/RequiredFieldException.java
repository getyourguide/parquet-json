package org.getyourguide.parquet.json;


import org.apache.parquet.ParquetRuntimeException;

public class RequiredFieldException extends ParquetRuntimeException {
  RequiredFieldException() {
    super();
  }

  RequiredFieldException(String message, Throwable cause) {
    super(message, cause);
  }

  RequiredFieldException(String message) {
    super(message);
  }

  RequiredFieldException(Throwable cause) {
    super(cause);
  }
}
