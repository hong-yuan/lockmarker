package com.lockmarker.api.exceptions;

public class TopicNotFoundException extends MsgasException {
    public TopicNotFoundException(String topicName) {
        super(topicName);
    }
}
