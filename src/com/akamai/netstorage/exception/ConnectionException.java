package com.akamai.netstorage.exception;

public class ConnectionException extends NetStorageException {
    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
