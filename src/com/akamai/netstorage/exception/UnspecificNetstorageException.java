package com.akamai.netstorage.exception;

public class UnspecificNetstorageException extends NetStorageException {
    final int responseCode;

    public UnspecificNetstorageException(int responseCode, String responseMessage) {
        super(responseMessage);
        this.responseCode = responseCode;
    }

    public UnspecificNetstorageException(String message) {
        this(-1, message);
    }

    public UnspecificNetstorageException(String message, Throwable cause) {
        super(message, cause);
        this.responseCode = -1;
    }
}