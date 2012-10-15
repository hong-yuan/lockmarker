package com.lockmarker.client;

public class MsgasClientFactory {
	public static MsgasClient getClient(String clientType, String serviceEndpoint) throws Exception {
		if (clientType.equals("JerseyClient")) {
			return new MsgasJerseyClient(serviceEndpoint);
		}
		else if (clientType.equals("JavaNetClient")) {
			return new MsgasJavaNetClient(serviceEndpoint);
		}
		else {
			throw new Exception("Invalid Msgas Client Type: " + clientType);
		}
	}
}
