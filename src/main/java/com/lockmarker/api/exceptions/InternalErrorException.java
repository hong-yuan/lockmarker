package com.lockmarker.api.exceptions;

public class InternalErrorException extends MsgasException {
    public InternalErrorException(String why) {
        super(why);
    }
}
