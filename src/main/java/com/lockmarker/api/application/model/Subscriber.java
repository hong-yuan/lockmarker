package com.lockmarker.api.application.model;

import java.util.HashSet;
import java.util.Arrays;
import java.util.Collection;

public class Subscriber {
	private String id;
	private String name;
	private String endpoint;
	private Collection<String> topics;
	
	public Subscriber(String id,
                          String name,
                          String endpoint,
                          Collection<String> topics) {
		this.id = id;
		this.name = name;
		this.endpoint = endpoint;
		this.topics = topics;
	}
	
	// for testing usage
	public Subscriber(String id,
                          String name,
                          String endpoint,
                          String[] topics) {
		this.id = id;
		this.name = name;
		this.endpoint = endpoint;
		this.topics = new HashSet<String>();
    	for (String t : Arrays.asList(topics)) {
    		this.topics.add(t);
        }
	}
	
	public String getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	
	public Collection<String> getTopics() {
		return topics;
	}
	
	public void addTopics(Collection<String> topics) {
		this.topics.addAll(topics);
	}
	
	public void removeTopics(Collection<String> topics) {
		this.topics.removeAll(topics);
	}
}
