package com.akamai.netstorage.exception;

public class AccessForbiddenException extends NetStorageException {
    public AccessForbiddenException(String message) {
        super(message);
    }

    public AccessForbiddenException(String message, Throwable cause) {
        super(message, cause);
    }
}
