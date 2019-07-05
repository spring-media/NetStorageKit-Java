package com.akamai.netstorage.exception;

public class LocalFileNotFoundException extends NetStorageException {
    public LocalFileNotFoundException(String message) {
        super(message);
    }

    public LocalFileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
