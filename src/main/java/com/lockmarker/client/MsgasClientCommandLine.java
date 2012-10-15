package com.lockmarker.client;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

/**
 * Run a service client to query Msgas REST API based on interactive input from users
 */
public class MsgasClientCommandLine {
	private static final String DEFAULT_SERVICE_HOST = "localhost";
	private static final String DEFAULT_SERVICE_PORT = "8080";
	private static final String DEFAULT_ENDPOINT_PREFIX = "http://";
	private static final String DEFAULT_ENDPOINT_SURFFIX = "/msgas";
	private static MsgasClient client = null;
	private static String endpoint = null;
	private static Console console = null;
	
	public static void main(String[] args) {
		try {
			console = System.console();
	        if (console == null) {
	            // if not in CLI console mode, simple run happy path on localhost
	        	endpoint = DEFAULT_ENDPOINT_PREFIX + DEFAULT_SERVICE_HOST + ":" + DEFAULT_SERVICE_PORT + DEFAULT_ENDPOINT_SURFFIX;
	        	client = MsgasClientFactory.getClient("JerseyClient", endpoint);
	        	showDemo();
	            System.exit(1);
	        }

	        printHeader();
	        setEndpoint();
	        client = MsgasClientFactory.getClient("JerseyClient", endpoint);
	        
	        while (true) {
		        int optIndex = getOperations();
		        System.out.println();
		        
		        switch (optIndex) {
		        	case 0: showDemo();
		        			break;
		        	case 1: listAllTopics();
	    					break;
		        	case 2: createTopic();
							break;
		        	case 3: describeTopic();
							break;
		        	case 4: deleteTopic();
							break;
		        	case 5: sendMessage();
							break;
		        	case 6: receiveMessage();
							break;
		        	case 7: deleteMessage();
							break;
		        	default: System.out.println("No operation found for Index " + optIndex);
		        	         confirmQuit();
	                		 break;
		        }
		        confirmQuit();
	        }
        } catch (Exception e) {
        	System.exit(1);
        }
	}
	
	private static void setEndpoint() {
		try {
	        String host = console.readLine("Please enter API Server's IP/hostname (Default: localhost): ");
	        if (null == host || host.isEmpty()) {
	        	host = DEFAULT_SERVICE_HOST;
	        }
	        endpoint = DEFAULT_ENDPOINT_PREFIX + host.trim() + ":" + DEFAULT_SERVICE_PORT + DEFAULT_ENDPOINT_SURFFIX;
	        System.out.println("Service endpoint is set as: " + endpoint);
	        System.out.println();
        } catch (Exception e) {
        	System.err.println("Failed to read your input! Try again later.");
        }
	}
	
	private static int getOperations() {
		int i = 0;
		List<String> operations = new ArrayList<String>();
		operations.add((i++) + ". show demo");
		operations.add((i++) + ". list all topics");
		operations.add((i++) + ". create a topic");
		operations.add((i++) + ". describe a topic");
		operations.add((i++) + ". delete topic");
		operations.add((i++) + ". send a message");
		operations.add((i++) + ". receive a message");
		operations.add((i++) + ". delete a message");
		
		System.out.println("\n\nMessaging operations to run: ");
		for (String opt: operations) {
            System.out.println("   " + opt);
        }
		System.out.println();
		
		while(true) {
			int selected = 0;
			String input = console.readLine("Please select an operation to execute (Default: 0): ");
			
			if (null == input || input.isEmpty()) {
				return selected;
			}
			
			try {
				selected = Integer.parseInt(input);
			} catch(Exception e) {
				System.err.println("Invalid input. Enter a number in the operation list!");
				continue;
			}
			if (selected >= 0 && selected <= operations.size()) {
				return selected;
			}
			else {
				System.err.println("Invalid input. Enter a number in the operation list!");
			}
		}
	}
	
	private static void confirmQuit() {
		String input = console.readLine("\n\nWant to play more (y/n)? (Default: y): ");
		if (input.equalsIgnoreCase("n")) {
			System.out.println("\nThanks for using HPCS Messaging as a Service. See you later!\n\n");
			System.exit(1);
		}
	}
	
	private static void printHeader() {
		System.out.println("+-----------------------------+");
		System.out.println("| HPCS Messaging as a Service |");
		System.out.println("+-----------------------------+");
		System.out.println();
	}
	
	private static void listAllTopics() {
		client.getTopics();
		System.out.println();
	}
	
	private static void createTopic() {
		String topicName = null;
		while (null == topicName || topicName.isEmpty()) {
			topicName = console.readLine("Enter topic name to create: ");
		}
		client.createTopic(topicName);
		System.out.println();
	}
	
	private static void describeTopic() {
		String topicName = null;
		while (null == topicName || topicName.isEmpty()) {
			topicName = console.readLine("Enter topic name to describe: ");
		}
		client.describeTopic(topicName);
		System.out.println();
	}

	private static void deleteTopic() {
		String topicName = null;
		while (null == topicName || topicName.isEmpty()) {
			topicName = console.readLine("Enter topic name to delete: ");
		}
		client.deleteTopic(topicName);
		System.out.println();
	}
	
	private static void sendMessage() {
		String topic = null;
		String message = null;
		while (null == topic || topic.isEmpty()) {
			topic = console.readLine("Enter topic name to send message: ");
		}
		while (null == message || message.isEmpty()) {
			message = console.readLine("Enter message body to send: ");
		}
		client.sendMessage(topic, message);
		System.out.println();
	}
	
	private static void receiveMessage() {
		String topic = null;
		while (null == topic || topic.isEmpty()) {
			topic = console.readLine("Enter topic name to pull message: ");
		}
		client.pullMessage(topic);
		System.out.println();
	}
	
	private static void deleteMessage() {
		String topic = null;
		String messageId = null;
		while (null == topic || topic.isEmpty()) {
			topic = console.readLine("Enter topic name to delete message: ");
		}
		while (null == messageId || messageId.isEmpty()) {
			messageId = console.readLine("Enter message ID to delete: ");
		}
		client.deleteMessage(topic, messageId);
		System.out.println();
	}
	
	/**
	 * Show non-interactive happy path
	 */
	private static void showDemo() {
		try {
			// create a topic
			client.createTopic("DemoTopic");
			System.out.println();
			
			client.describeTopic("DemoTopic");
			System.out.println();
			
			// publish two messages to the topic
			client.sendMessage("DemoTopic", "Hello World! Message #1.");
			System.out.println();
			
			client.sendMessage("DemoTopic", "Hello World! Message #2.");
			System.out.println();
			
			// consume one message
			client.pullMessage("DemoTopic");
			System.out.println();
			
			// show messages in the topic 
			client.getTopics();
			System.out.println();

			// create one more topic
			client.createTopic("AnotherDemoTopic");
			System.out.println();

			// list all topics
			client.getTopics();
			System.out.println();

			// delete all topics
			client.deleteTopic("DemoTopic");
			System.out.println();
			
			client.deleteTopic("AnotherDemoTopic");
			System.out.println();
			
			client.getTopics();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
