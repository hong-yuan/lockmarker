package com.lockmarker.api.application.model;

public class Message {
	private String id;
	private String topic;
	private byte[] body;
	
	public Message(String id,
                   String topic,
                   byte[] body) {
		this.id = id;
		this.topic = topic;
		this.body = body;
	}
	
	public String getId() {
		return id;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public byte[] getBody() {
		return body;
	}
}
