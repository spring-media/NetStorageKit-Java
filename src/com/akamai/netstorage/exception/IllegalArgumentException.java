package com.akamai.netstorage.exception;

public class IllegalArgumentException extends NetStorageException {
    public IllegalArgumentException(String message) {
        super(message);
    }

    public IllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }
}
