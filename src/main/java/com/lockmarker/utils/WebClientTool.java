package com.lockmarker.utils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

import com.yammer.dropwizard.logging.Log;

public class WebClientTool {
	private static final Log LOG = Log.forClass(WebClientTool.class);
	
	public static boolean isEndpointAlive(String endpoint) {
    	try {
    		LOG.debug("Validating aliveness of endpoint: " + endpoint);
    		// Simply ping the endpoint using PUT w/o actual request body
			ClientResponse response = Client.create().resource(endpoint).put(ClientResponse.class);
			if (response.getStatus() != 200) {
				LOG.info("Invalid endpoint: " + endpoint);
				return false;
			}
			LOG.info("Confirmed aliveness of endpoint: " + endpoint);
			return true;
		} catch (Exception e) {
			LOG.error("Error validating endpoint: " + endpoint);
			return false;
		}
	}
}
