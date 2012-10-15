package com.lockmarker.api.exceptions;

public class MessageNotFoundException extends MsgasException {
    //public MessageNotFoundException(String topicName) {
    //    super(topicName);
    //}
    public MessageNotFoundException() {
        super("no available message");
    }
}
