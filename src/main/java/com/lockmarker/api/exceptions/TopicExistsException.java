package com.lockmarker.api.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: dragosmanolescu
 * Date: 6/6/12
 * Time: 9:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class TopicExistsException extends MsgasException {
    public TopicExistsException(String name) {
        super(name);
    }
}
