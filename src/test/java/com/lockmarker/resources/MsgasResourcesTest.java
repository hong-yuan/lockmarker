package com.lockmarker.resources;

import com.lockmarker.api.application.MessagingDispatcher;
import com.lockmarker.api.application.model.Message;
import com.lockmarker.api.application.model.Subscriber;
import com.yammer.dropwizard.testing.ResourceTest;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

/**
 * Unit tests on Messaging Service resources independent of resources processing implementation
 */
public class MsgasResourcesTest extends ResourceTest {

	private static final String tenantId = "12345678901234";
	private static final MessagingDispatcher mockMsgDispatcher = mock(MessagingDispatcher.class);
	private static final LockMarkerResource msgasServer = new LockMarkerResource(mockMsgDispatcher);
	private static final String serviceEndpoint = "/v1.0/" + tenantId + "/";
	private static final String testTopicName = "test-topic";
	private static final String testTextMessage = "test text message";
	private static final String testMessageId = "12345";
	private static final Message testMessageObj = new Message(testMessageId, 
			                                                  testTopicName,
			                                                  testTextMessage.getBytes());
	private static final String testSubscriberId = "12345";
	private static final String testSubscriberName = "test-sub";
	private static final String testEndpoint = "http://www.test.com:8081/listener";
	private static final String[] testSubscribers = {"123", "456", "789"};
	private static final String[] testTopics = {"topic1", "topic2", "topic3"};
    private static final String[] testSubscribedTopics = {"topic1", "topic3"};
    private static final String[] testUnsubscribedTopics = {"topic2", "topic3"};
    private static final Subscriber TEST_SUBSCRIBER_INFO = new Subscriber(testSubscriberId,
														            testSubscriberName, 
														            testEndpoint,
														            testTopics);
    private static final Subscriber TEST_CREATE_SUBSCRIBER = new Subscriber(testSubscriberId,
														            testSubscriberName, 
														            testEndpoint,
														            testSubscribedTopics);
    
	
    @Override
    protected void setUpResources() {
    	when(mockMsgDispatcher.getTopics(tenantId)).thenReturn(Arrays.asList(testTopics));
    	when(mockMsgDispatcher.describeTopic(tenantId, testTopicName)).thenReturn(Arrays.asList(testSubscribers));
    	mockMsgDispatcher.createTopic(tenantId, testTopicName);
    	mockMsgDispatcher.deleteTopic(tenantId, testTopicName);
    	
    	when(mockMsgDispatcher.sendMessage(tenantId, testTopicName, testTextMessage)).thenReturn(testMessageId);
    	when(mockMsgDispatcher.pullMessage(tenantId, testTopicName)).thenReturn(testMessageObj);
    	when(mockMsgDispatcher.deleteMessage(tenantId, testTopicName, testMessageId)).thenReturn(true);
    	
    	when(mockMsgDispatcher.createSubscriber(testSubscriberName, testEndpoint, Arrays.asList(testTopics)))
    		.thenReturn(TEST_CREATE_SUBSCRIBER);
    	when(mockMsgDispatcher.getSubscriberInfo(testSubscriberId)).thenReturn(TEST_SUBSCRIBER_INFO);
    	when(mockMsgDispatcher.deleteSubscriber(testSubscriberId)).thenReturn(true);
    	when(mockMsgDispatcher.subscribeTopic(testSubscriberId, Arrays.asList(testTopics))).thenReturn(Arrays.asList(testUnsubscribedTopics));
    	when(mockMsgDispatcher.unsubscribeTopic(testSubscriberId, Arrays.asList(testTopics))).thenReturn(Arrays.asList(testUnsubscribedTopics));

      addResource(msgasServer);
    }

    private ArrayNode createTopicListNodes(String[] testTopics) {
   	 ArrayNode topicList = JsonNodeFactory.instance.arrayNode();
   	 for (String t : Arrays.asList(testTopics)) {
   		 topicList.add(t);
   	 }
   	 return topicList;
    }
    
    private ArrayNode createTopicListNodes(Collection<String> testTopics) {
    	ArrayNode topicList = JsonNodeFactory.instance.arrayNode();
        for (String t : testTopics) {
        	topicList.add(t);
        }
        return topicList;
    }
    
    private ArrayNode createSubscriberListNodes(String[] testSubscribers) {
    	ArrayNode subscriberList = JsonNodeFactory.instance.arrayNode();
    	for (String s : Arrays.asList(testSubscribers)) {
        	subscriberList.add(s);
        }
        return subscriberList;
    }
    

    // @Test
    public void testListTopics() throws Exception {
        ObjectNode response = JsonNodeFactory.instance.objectNode();
        response.put("status", 200);
        ArrayNode topicList = createTopicListNodes(testTopics);
        response.put("topics", topicList);

        assertThat("Test list topics",
                client().resource(serviceEndpoint + "topic")
                        .type("application/json")
                        .accept("application/json")
                        .get(ObjectNode.class),
                equalTo(response));
    }

    // @Test
    public void testDescribeTopic() throws Exception {
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("topic", testTopicName);
    	response.put("status", 200);
    	ArrayNode subscriberList = createSubscriberListNodes(testSubscribers);
        response.put("subscribers", subscriberList);
    	
    	assertThat("Test describe a topic",
     			client().resource(serviceEndpoint + "topic/" + testTopicName)
     				.type("application/json")
     				.accept("application/json")
                    .get(ObjectNode.class),
                equalTo(response));
    }

    // @Test
    public void testCreateTopic() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("name", testTopicName);
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("topic", testTopicName);

    	assertThat("Test create a topic",
     			client().resource(serviceEndpoint + "topic")
     				.type("application/json")
     				.accept("application/json")
     				.post(ObjectNode.class, request),
                equalTo(response));
    }
    
    // @Test
    public void testDeleteTopic() throws Exception {
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("topic", testTopicName);

    	assertThat("Test delete a topic",
     			client().resource(serviceEndpoint + "topic/" + testTopicName)
     				.type("application/json")
     				.accept("application/json")
     				.delete(ObjectNode.class),
                equalTo(response));
    }
    
    // @Test
    public void testSendMessage() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("command", "send");
    	request.put("message", testTextMessage);
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("messageId", testMessageId);

    	assertThat("Test send a message",
     			client().resource(serviceEndpoint + "topic/" + testTopicName)
     				.type("application/json")
     				.accept("application/json")
     				.post(ObjectNode.class, request),
                equalTo(response));
    }
    
    // @Test
    public void testReceiveMessage() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("command", "receive");
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("messageId", testMessageId);
    	response.put("topicName", testTopicName);
    	response.put("message", testTextMessage);

    	assertThat("Test receive a message",
     			client().resource(serviceEndpoint + "topic/" + testTopicName)
     				.type("application/json")
     				.accept("application/json")
     				.post(ObjectNode.class, request),
                equalTo(response));
    }
    
    // @Test
    public void testDeleteMessage() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("command", "delete");
    	request.put("messageId", testMessageId);
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("messageId", testMessageId);

    	assertThat("Test delete a message",
     			client().resource(serviceEndpoint + "topic/" + testTopicName)
     				.type("application/json")
     				.accept("application/json")
     				.post(ObjectNode.class, request),
                equalTo(response));
    }
    
    // @Test
    public void testCreateSubscriber() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("subscriber", testSubscriberName);
    	request.put("endpoint", testEndpoint);
    	ArrayNode topicList = createTopicListNodes(testTopics);
        request.put("topics", topicList);
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("subscriberId", testSubscriberId);
        ArrayNode subscriptions = response.putArray("subscribedTo");
        subscriptions.add("topic1");
        subscriptions.add("topic3");
    	
    	assertThat("Test create a subscriber",
    			client().resource(serviceEndpoint + "subscriber")
     				.type("application/json")
     				.accept("application/json")
     				.put(ObjectNode.class, request),
                equalTo(response));
    }
    
    // @Test
    public void testGetSubscriberInfo() throws Exception {
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("id", TEST_SUBSCRIBER_INFO.getId());
    	response.put("name", TEST_SUBSCRIBER_INFO.getName());
    	response.put("endpoint", TEST_SUBSCRIBER_INFO.getEndpoint());
    	response.put("subscribedTo", createTopicListNodes(TEST_SUBSCRIBER_INFO.getTopics()));

    	assertThat("Test describe a subscriber",
     			client().resource(serviceEndpoint + "subscriber/" + testSubscriberId)
     				.accept("application/json")
     				.get(ObjectNode.class),
                equalTo(response));
    }
    
    // @Test
    public void testDeleteSubscriber() throws Exception {
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
    	response.put("subscriberId", testSubscriberId);

    	assertThat("Test delete a subscriber",
     			client().resource(serviceEndpoint + "subscriber/" + testSubscriberId)
     				.type("application/json")
     				.accept("application/json")
     				.delete(ObjectNode.class),
                equalTo(response));
    }
    
    // @Test
    public void testSubscribeTopic() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("command", "subscribe");
    	ArrayNode topicList = createTopicListNodes(testTopics);
        request.put("topics", topicList);
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
        ArrayNode subscriptions = response.putArray("subscribedTo");
        subscriptions.add("topic2");
        subscriptions.add("topic3");

    	assertThat("Test subscribe to topics",
    			client().resource(serviceEndpoint + "subscriber/" + testSubscriberId)
     				.type("application/json")
     				.accept("application/json")
     				.post(ObjectNode.class, request),
                equalTo(response));
    }
    
    // @Test
    public void testUnsubscribeTopic() throws Exception {
    	ObjectNode request = JsonNodeFactory.instance.objectNode();
    	request.put("command", "unsubscribe");
    	ArrayNode topicList = createTopicListNodes(testTopics);
        request.put("topics", topicList);
    	
    	ObjectNode response = JsonNodeFactory.instance.objectNode();
    	response.put("status", 200);
        ArrayNode subscriptions = response.putArray("unsubscribedFrom");
        subscriptions.add("topic2");
        subscriptions.add("topic3");

    	assertThat("Test unsubscribe to topics",
    			client().resource(serviceEndpoint + "subscriber/" + testSubscriberId)
     				.type("application/json")
     				.accept("application/json")
     				.post(ObjectNode.class, request),
                equalTo(response));
    }
}
