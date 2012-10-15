package com.lockmarker.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
 
public class MsgasJerseyClient implements MsgasClient {

	private final String serviceEndpoint;
	
	public MsgasJerseyClient(String serviceEndpoint) {
        this.serviceEndpoint = serviceEndpoint;
    }

	public void getTopics() {
		try {
			System.out.println("----- Getting all topics -----");

			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic");
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			String output = response.getEntity(String.class);
			System.out.println(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void createTopic(String topicName) {
		try {
			System.out.println("----- Creating Topic \"" + topicName + "\" -----");
		
			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic");
			String input = "{\"name\":\"" + topicName + "\"}";
			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);
 
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			String output = response.getEntity(String.class);
			System.out.println(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void deleteTopic(String topicName) {
		try {
			System.out.println("----- Deleting Topic \"" + topicName + "\" -----");
		
			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic/" + topicName);
			ClientResponse response = webResource.type("text/plain").delete(ClientResponse.class);
 
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			String output = response.getEntity(String.class);
			System.out.println(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void describeTopic(String topicName) {
		try {
			System.out.println("----- Describing Topic \"" + topicName + "\"-----");

			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic/" + topicName);
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			String output = response.getEntity(String.class);
			System.out.println(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void sendMessage(String topic, String message) {
		try {
			System.out.println("----- Sending a message (\"" + message + "\") to Topic \"" + topic + "\" -----");
		
			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic/" + topic);
			String input = "{\"command\":\"send\", \"message\":\"" + message + "\"}";
			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);
 
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			String output = response.getEntity(String.class);
			System.out.println(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void pullMessage(String topic) {
		try {
			System.out.println("----- Pulling a message from Topic \"" + topic + "\" -----");
		
			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic/" + topic);
			String input = "{\"command\":\"receive\"}";
			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);
 
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			
			String body = response.getEntity(String.class);
			System.out.println(body);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void deleteMessage(String topic, String messageId) {
		try {
			System.out.println("----- Pulling a message from Topic \"" + topic + "\" -----");
		
			Client client = Client.create();
			WebResource webResource = client.resource(serviceEndpoint + "/topic/" + topic);
			String input = "{\"command\":\"delete\", \"messageId\":\"" + messageId + "\"}";
			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);
 
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + response.getStatus());
			}
			
			String body = response.getEntity(String.class);
			System.out.println(body);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
