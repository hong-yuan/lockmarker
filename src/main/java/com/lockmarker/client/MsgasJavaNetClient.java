package com.lockmarker.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ConnectException;
import java.net.URL;
 
public class MsgasJavaNetClient implements MsgasClient {
	
	private final String serviceEndpoint;
	
	public MsgasJavaNetClient(String serviceEndpoint) {
        this.serviceEndpoint = serviceEndpoint;
    }
	
	public void getTopics() {
		try {
			System.out.println("----- Getting all topics -----");

			URL url = new URL(serviceEndpoint + "/topic");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: "
						+ conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTopic(String topicName) {
		try {
			System.out.println("----- Creating Topic \"" + topicName + "\" -----");

			URL url = new URL(serviceEndpoint + "/topic");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setDoOutput(true);
			
			String input = "{\"name\":\"" + topicName + "\"}";

			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();
	 
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: " + conn.getResponseCode());
			}
	 
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteTopic(String topicName) {
		try {
			System.out.println("----- Deleting Topic \"" + topicName + "\" -----");

			URL url = new URL(serviceEndpoint + "/topic/" + topicName);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: "
						+ conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void describeTopic(String topicName) {
		try {
			System.out.println("----- Describing Topic \"" + topicName + "\"-----");

			URL url = new URL(serviceEndpoint + "/topic/" + topicName);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: "
						+ conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void sendMessage(String topic, String message) {
		try {
			System.out.println("----- Sending a message (\"" + message + "\") to Topic \"" + topic + "\" -----");

			URL url = new URL(serviceEndpoint + "/topic/" + topic);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setDoOutput(true);

			String input = "{\"command\":\"send\", \"message\":\"" + message + "\"}";
			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();
			
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: "
						+ conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void pullMessage(String topic) {
		try {
			System.out.println("----- Pulling a message from Topic \"" + topic + "\" -----");

			URL url = new URL(serviceEndpoint + "/topic/" + topic);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setDoOutput(true);
			
			String input = "{\"command\":\"receive\"}";
			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();
			
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: "
						+ conn.getResponseCode());
			}
			
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteMessage(String topic, String messageId) {
		try {
			System.out.println("----- Pulling a message from Topic \"" + topic + "\" -----");

			URL url = new URL(serviceEndpoint + "/topic/" + topic);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setDoOutput(true);
			
			String input = "{\"command\":\"delete\", \"messageId\":\"" + messageId + "\"}";
			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();
			
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed with HTTP error code: "
						+ conn.getResponseCode());
			}
			
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ConnectException e) {
			System.out.println("Error connecting to Msgas Service. Please make sure the REST server is running.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}