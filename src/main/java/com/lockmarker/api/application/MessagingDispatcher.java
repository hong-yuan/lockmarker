package com.lockmarker.api.application;

import java.util.Collection;

import com.lockmarker.api.application.model.Subscriber;
import com.lockmarker.api.application.model.Message;
import com.lockmarker.config.MessagingConfiguration;

/**
 * Created with IntelliJ IDEA.
 * User: dragosmanolescu
 * Date: 7/5/12
 * Time: 1:40 PM
 * To change this template use File | Settings | File Templates.
 */
public interface MessagingDispatcher {
	public void loadConfiguration(MessagingConfiguration configuration);
	
    public Collection<String> getTopics(String tenantId);
    public Collection<String> describeTopic(String tenantId, String topicName);
    public void createTopic(String tenantId, String name);
    public void deleteTopic(String tenantId, String name);
    
    public String sendMessage(String tenantId, String topicName, String message);
    public Message pullMessage(String tenantId, String topicName);     // only for P2P message polling
    public boolean deleteMessage(String tenantId, String topicName, String messageId);
    
    public Subscriber createSubscriber(String subscriberName, String endpoint, Collection<String> topics);
    public Subscriber getSubscriberInfo(String subscriberId);
    public boolean deleteSubscriber(String subscriberId);
    public Collection<String> subscribeTopic(String subscriberId, Collection<String> topics);
    public Collection<String> unsubscribeTopic(String subscriberId, Collection<String> topics);
}
