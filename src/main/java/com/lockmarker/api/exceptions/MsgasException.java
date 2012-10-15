package com.lockmarker.api.exceptions;

public abstract class MsgasException extends RuntimeException {
    public MsgasException(String name) {
        super(name);
    }
}
