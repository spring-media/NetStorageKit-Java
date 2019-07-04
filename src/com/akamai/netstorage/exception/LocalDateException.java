package com.akamai.netstorage.exception;

public class LocalDateException extends NetStorageException {
    public LocalDateException(String message) {
        super(message);
    }

    public LocalDateException(String message, Throwable cause) {
        super(message, cause);
    }
}
