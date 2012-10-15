package com.lockmarker.api.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dragosmanolescu
 * Date: 8/28/12
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class AuthenticationException extends MsgasException {
    public AuthenticationException(String name) {
        super(name);
    }
}
