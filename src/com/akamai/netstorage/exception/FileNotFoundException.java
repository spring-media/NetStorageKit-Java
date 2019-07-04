package com.akamai.netstorage.exception;

public class FileNotFoundException extends NetStorageException {
    public FileNotFoundException(String message) {
        super(message);
    }

    public FileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
