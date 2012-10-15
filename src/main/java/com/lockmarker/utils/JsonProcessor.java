package com.lockmarker.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;

import com.lockmarker.api.exceptions.InternalErrorException;

import com.yammer.dropwizard.logging.Log;

public class JsonProcessor {
	private static final Log LOG = Log.forClass(JsonProcessor.class);
	
	public static String jgetString(JsonNode node,
			                         String name) {
    	JsonNode jsonNode = node.get(name);
        if (jsonNode == null) throw new IllegalArgumentException(name + " not specified");
        String s = jsonNode.getTextValue();
        if (s.equals("")) throw new IllegalArgumentException(name + " not specified");
        return s;
    }
    
    public static Collection<String> jgetChildrenInString(JsonNode node,
    		                                               String root) {
        if (!node.has(root))
            throw new IllegalArgumentException("Node " + node.toString() + " has no element named " + root);
        JsonNode parent = node.path(root);
    	Collection<String> children = new ArrayList<String>();
		Iterator<JsonNode> ite = parent.getElements();

		while (ite.hasNext()) {
			JsonNode temp = ite.next();
			children.add(temp.getTextValue());
		}
    	return children;
    }
	
	public static Collection<String> parseJsonList(String jsonInput,
		                                           String keyToSearch, 
			                                       Collection<String> valuesToSkip) {
		try {
			if (null == jsonInput) {
				LOG.debug("Cannot parsing null input Json string.");
				return null;
			}
			// LOG.debug("Parsing json string: " + jsonInput);

			Collection<String> result = new HashSet<String>();
			JsonFactory jfactory = new JsonFactory();
			JsonParser jParser = jfactory.createJsonParser(jsonInput);

			// skip START_ARRAY '['
			if (jParser.nextToken() == JsonToken.START_ARRAY) {
				jParser.nextToken();
			}
			// loop until END_ARRAY ']'
			while (jParser.nextToken() != JsonToken.END_ARRAY) {
				String fieldname = jParser.getCurrentName();
				// find the key
				if (keyToSearch.equals(fieldname)) {
					// get the value
					jParser.nextToken();
					String tokenValue = jParser.getText();

					// validate the value
					boolean skip = false;
					// ignore empty value
					if (tokenValue.isEmpty()) {
						skip = true;
					} else if (null != valuesToSkip) {
						for (String v : valuesToSkip) {
							// broad match
							if (v.contains("*")) {
								if (tokenValue.startsWith(v.substring(0,
										v.length() - 1))) {
									skip = true;
								}
							} else {
								// exact match
								if (tokenValue.equals(v)) {
									skip = true;
								}
							}
						}
					}

					// add the valid value to result list
					if (!skip) {
						result.add(tokenValue);
					}
					jParser.skipChildren();
				}
			}
			jParser.close();
			return result;
		} catch (JsonGenerationException e) {
			LOG.error(e, "Error parsing RMQ Json response.");
			throw new InternalErrorException("Error parsing RMQ Json response.");
		} catch (JsonMappingException e) {
			LOG.error(e, "Error parsing RMQ Json response.");
			throw new InternalErrorException("Error parsing RMQ Json response.");
		} catch (IOException e) {
			LOG.error(e, "Error parsing RMQ Json response.");
			throw new InternalErrorException("Error parsing RMQ Json response.");
		}
	}

	public static Map<String, String> parseJsonEntities(String jsonInput,
			                                            Collection<String> keysToSearch) {
		try {
			if (null == jsonInput) {
				LOG.debug("Cannot parsing null input Json string.");
				return null;
			}
			// LOG.debug("Parsing json string: " + jsonInput);

			Map<String, String> result = new HashMap<String, String>();
			JsonFactory jfactory = new JsonFactory();
			JsonParser jParser = jfactory.createJsonParser(jsonInput);

			while (null != jParser.nextToken()) {
				String fieldname = jParser.getText();
				for (String key : keysToSearch) {
					if (key.equals(fieldname)) {
						jParser.nextToken();
						String value = jParser.getText();
						result.put(key, value);
						LOG.debug(key + " = " + value);
					}
				}
			}
			jParser.close();
			return result;
		} catch (JsonGenerationException e) {
			LOG.error(e, "Error parsing RMQ Json response.");
			throw new InternalErrorException("Error parsing RMQ Json response.");
		} catch (JsonMappingException e) {
			LOG.error(e, "Error parsing RMQ Json response.");
			throw new InternalErrorException("Error parsing RMQ Json response.");
		} catch (IOException e) {
			LOG.error(e, "Error parsing RMQ Json response.");
			throw new InternalErrorException("Error parsing RMQ Json response.");
		}
	}
}
