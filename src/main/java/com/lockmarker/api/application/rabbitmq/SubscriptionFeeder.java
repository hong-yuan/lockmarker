package com.lockmarker.api.application.rabbitmq;

import com.lockmarker.api.application.model.Message;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.yammer.dropwizard.logging.Log;

/**
 * This class implements all logic for feeding message to subscribers 
 * upon receiving message from RabbitMQ client.
 */
public class SubscriptionFeeder {
	private static final Log LOG = Log.forClass(SubscriptionFeeder.class);
	private String subscriberName;
	private String subscriberId;
	private String endpoint;
	private Client webclient;
	private WebResource webResource;
	
	SubscriptionFeeder(String subscriberName,
			           String subscriberId,
			           String endpoint) {
		this.subscriberName = subscriberName;
		this.subscriberId = subscriberId;
		this.endpoint = endpoint;
		this.webclient = Client.create();
		this.webResource = webclient.resource(endpoint);
	}

	public void feed(Message message) {
		try {
			LOG.debug("Feeding message " + message.getId() + "to subscriber " + subscriberName);
			ObjectNode feedJson = JsonNodeFactory.instance.objectNode();
			feedJson.put("messageId", message.getId());
			feedJson.put("topicName", message.getTopic());
			feedJson.put("message", new String(message.getBody()));
			
			ClientResponse response = webResource.put(ClientResponse.class, feedJson.toString());
			if (response.getStatus() != 200) {
				LOG.debug("Failed feeding message to subscription");
				LOG.debug("\t\tmessageId = " + message.getId());
				LOG.debug("\t\ttopic = " + message.getTopic());
				LOG.debug("\t\tsubscriberName = " + subscriberName);
				LOG.debug("\t\tsubscriberId = " + subscriberId);
				LOG.debug("\t\tendpoint = " + endpoint);
				LOG.debug("endpoint response code: " + response.getStatus());
				LOG.error("Error feeding message to suscriber.");
				throw new RuntimeException("Error feeding message to Subscriber " +
						subscriberName + " (" + subscriberId + 
						"). Endpoint response code: " + response.getStatus());
			}
		} catch (Exception e) {
			LOG.error("Error feeding message " + message.getId() + " to Suscriber " + subscriberId);
			throw new RuntimeException("Failed feeding message to Subscriber " 
					+ subscriberName 
					+ " (" + subscriberId + ").");
		}
	}
	
	public String getSubscriberId() {
		return subscriberId;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
}
