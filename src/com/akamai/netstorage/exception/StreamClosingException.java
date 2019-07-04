package com.akamai.netstorage.exception;

public class StreamClosingException extends NetStorageException {
    public StreamClosingException(String message) {
        super(message);
    }

    public StreamClosingException(String message, Throwable cause) {
        super(message, cause);
    }
}
