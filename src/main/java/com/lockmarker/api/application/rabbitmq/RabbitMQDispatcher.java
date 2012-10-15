package com.lockmarker.api.application.rabbitmq;

import com.lockmarker.api.application.MessagingDispatcher;
import com.lockmarker.api.application.model.Subscriber;
import com.lockmarker.api.application.model.Message;
import com.lockmarker.api.application.policy.RedeliveryPolicy;
import com.lockmarker.api.exceptions.InternalErrorException;
import com.lockmarker.config.MessagingConfiguration;
import com.lockmarker.config.RabbitMQConfiguration;
import com.lockmarker.utils.JsonProcessor;
import com.lockmarker.utils.WebClientTool;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.yammer.dropwizard.logging.Log;

public class RabbitMQDispatcher implements MessagingDispatcher {
	private static final Log		LOG						= Log.forClass(RabbitMQDispatcher.class);
	private String						RABBITMQ_HOST			= "localhost";
	private int							RABBITMQ_PORT			= 5672;
	private String						RABBITMQ_USERID		= "guest";
	private String						RABBITMQ_PASSWORD		= "guest";
	private String						RABBITMQ_VHOST			= "/";
	private String						RABBITMQ_WEBAPI_PORT	= "55672";
	private String						RABBITMQ_WEBAPI		= "http://" +
																		  RABBITMQ_HOST +
																		  ":" +
																		  RABBITMQ_WEBAPI_PORT +
																		  "/api/";
	private static final String	DEFAULT_ROUTING_KEY	= "#";
	private static final String	RESERVED_P2P_PREFIX	= "MSGAS-P2P";
	private Connection				connection;
	private Channel					defaultChannel;
	private Client						webclient;
	private RedeliveryPolicy		defaultRetryPolicy;
	private Map<String, Channel>  activeChannels			= null;

	/**
	 * This constructor is needed for Guice dependency injection The connection
	 * to RabbitMQ server is triggered by caller calling loadConfiguration()
	 */
	public RabbitMQDispatcher() {
	}

	private void init() {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(RABBITMQ_HOST);
			factory.setPort(RABBITMQ_PORT);
			factory.setUsername(RABBITMQ_USERID);
			factory.setPassword(RABBITMQ_PASSWORD);
			factory.setVirtualHost(RABBITMQ_VHOST);
			this.connection = factory.newConnection();
			this.defaultChannel = connection.createChannel();
			this.webclient = Client.create();
			this.webclient.addFilter(new HTTPBasicAuthFilter(RABBITMQ_USERID, RABBITMQ_PASSWORD));
			this.defaultRetryPolicy = new RedeliveryPolicy();
			this.activeChannels = new ConcurrentHashMap<String, Channel>();

			LOG.debug("Connected to RabbitMQ server with the following parameters:");
			LOG.debug("host = " + RABBITMQ_HOST);
			LOG.debug("port = " + RABBITMQ_PORT);
			LOG.debug("user = " + RABBITMQ_USERID);
			LOG.debug("password = " + RABBITMQ_PASSWORD);
			LOG.debug("vhost = " + RABBITMQ_VHOST);
		} catch (Exception e) {
			LOG.error(e, "Error initializing RabbitMQ dispatcher.");
			throw new InternalErrorException("Error starting messaging service.");
		} finally {
			// this.connection.close();
		}
	}

	@Override
	public void loadConfiguration(MessagingConfiguration configuration) {
		RabbitMQConfiguration config = configuration.getRabbitMQConfiguration();
		RABBITMQ_HOST = config.getHost();
		RABBITMQ_PORT = config.getPort();
		RABBITMQ_USERID = config.getUser();
		RABBITMQ_PASSWORD = config.getPassword();
		RABBITMQ_VHOST = config.getVhost();
		RABBITMQ_WEBAPI = "http://" + RABBITMQ_HOST + ":" + RABBITMQ_WEBAPI_PORT + "/api/";
		init();
	}

	@Override
	public Collection<String> getTopics(String tenantId) {
		try {
			String uri = RABBITMQ_WEBAPI + "exchanges/" + tenantId;
			WebResource webResource = this.webclient.resource(uri);
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

			int statusCode = response.getStatus();
			if (statusCode != 200) {
				LOG.error("Error getting topic list from RMQ server. Status code: " + statusCode);
				throw new InternalErrorException("Error listing topics.");
			}

			// need to skip all RMQ built-in exchanges
			Collection<String> valuesToSkip = new ArrayList<String>();
			valuesToSkip.add("direct");
			valuesToSkip.add("amq.*"); // * is wildcard here
			Collection<String> topics = JsonProcessor.parseJsonList(response.getEntity(String.class),
																						"name",
																						valuesToSkip);
			LOG.debug("Topic list retrieved from RMQ: " + topics.toString());

			return topics;
		} catch (Exception e) {
			LOG.error(e, "Error listing topics.");
			throw new InternalErrorException("Error listing topics.");
		}
	}

	@Override
	public void createTopic(String tenantId, String topicName) {
		try {
			// declare durable and non-autodelete exchange to represent the topic 
			Channel channel = null;
			channel.exchangeDeclare(topicName, "topic", true);

			// always bind a P2P queue to the topic
			String p2pQueueName = RESERVED_P2P_PREFIX + "-" + topicName;
			channel.queueDeclare(p2pQueueName, true, false, false, null);
			channel.queueBind(p2pQueueName, topicName, DEFAULT_ROUTING_KEY);
			LOG.debug("Created topic: {}", topicName);
		} catch (Exception e) {
			LOG.error(e, "Error creating topic: {}", topicName);
			throw new InternalErrorException("Error creating topic" + topicName);
		}
	}

	@Override
	public Collection<String> describeTopic(String tenantId, String topicName) {
		try {
			// Refer RMQ API at
			// http://hg.rabbitmq.com/rabbitmq-management/raw-file/rabbitmq_v2_8_5/priv/www/api/index.html
			String uri = RABBITMQ_WEBAPI +
								"exchanges/" +
								(tenantId.equals("/") ? "%2f" : tenantId) +
								"/" +
								topicName +
								"/bindings/source";
			WebResource webResource = this.webclient.resource(uri);
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

			int statusCode = response.getStatus();
			if (statusCode != 200) {
				LOG.error("Error getting subscription list from RMQ server. Status code: " + statusCode);
				throw new InternalErrorException("Error describing Topic " + topicName);
			}

			Collection<String> valuesToSkip = new ArrayList<String>();
			valuesToSkip.add("MSGAS-P2P-" + topicName);
			Collection<String> subscriptions = JsonProcessor.parseJsonList(
					response.getEntity(String.class), "destination", valuesToSkip);
			LOG.debug("Subscription list retrieved from RMQ for Topic {}: {}", topicName, subscriptions.toString());

			return subscriptions;
		} catch (Exception e) {
			LOG.error(e, "Error describing Topic {}.", topicName);
			throw new InternalErrorException("Error describing Topic " + topicName);
		}
	}

	@Override
	public void deleteTopic(String tenantId, String topicName) {
		try {
			// list current subscriptions of this topic and unsubscribe each of them
			Collection<String> subscriptions = describeTopic(tenantId, topicName);
			Channel tmpChannel = null;
			
			for (String subscriberId : subscriptions) {
				try {
					tmpChannel.queueUnbind(subscriberId, topicName,
							DEFAULT_ROUTING_KEY);
				} catch (Exception e) {
					LOG.error(e, "Error unbinding queue: " + subscriberId);
				}
			}
			// also unbind and delete the default P2P queue
			String p2pQueue = RESERVED_P2P_PREFIX + '-' + topicName;
			tmpChannel.queueUnbind(p2pQueue, topicName, DEFAULT_ROUTING_KEY);
			tmpChannel.queueDelete(p2pQueue);
			// finally delete the exchange of the topic
			tmpChannel.exchangeDelete(topicName);
			LOG.debug("Deleted topic: " + topicName);
		} catch (Exception e) {
			LOG.error(e, "Error deleting topic: {}", topicName);
			throw new InternalErrorException("Error deleting topic " + topicName);
		}
	}

	////////////////////
	// P2P operations //
	////////////////////
	@Override
	public String sendMessage(String tenantId, String topicName, String message) {
		try {
			BasicProperties prop = new BasicProperties();
			String messageId = UUID.randomUUID().toString();
			prop.setMessageId(messageId);

			Map<String, Object> topicHeader = new HashMap<String, Object>();
			topicHeader.put("topic", topicName);
			prop.setHeaders(topicHeader);

			// create a publisher channel
			Channel channel = null;
			channel.basicPublish(topicName, "", prop, message.getBytes());
			return messageId;
		} catch (Exception e) {
			LOG.error(e, "Error sending message to topic: " + topicName);
			throw new InternalErrorException("Error sending message to topic: " + topicName);
		}
	}

	@Override
	public Message pullMessage(String tenantId, String topicName) {
		Message message = null;
		try {
			// create a consumer channel for pulling message
			// Channel channel = connection.createChannel();
			String p2pQueueName = RESERVED_P2P_PREFIX + "-" + topicName;
			boolean autoAck = false;
			Channel channel = null;
			GetResponse response = channel.basicGet(p2pQueueName, autoAck);
			if (response == null) {
				LOG.debug("No message found from Topic: " + topicName);
			} else {
				String messageId = response.getProps().getMessageId();
				message = new Message(messageId, topicName, response.getBody());
				defaultChannel.basicAck(response.getEnvelope().getDeliveryTag(), false);
				LOG.debug("Message received. Message ID: " + messageId);
			}
			// channel.close();
		} catch (Exception e) {
			LOG.error(e, "Error pulling message from topic: " + topicName);
			throw new InternalErrorException("Error pulling message to topic: " + topicName);
		}
		return message;
	}

	@Override
	public boolean deleteMessage(String tenantId, String topicName, String id) {
		// TODO: it seems RabbitMQ allows only the consumption of message
		// but not deletion of message. Need to get this sorted out.
		return true;
	}

	// //////////////////////
	// Pub-Sub operations //
	// //////////////////////
	/**
	 * Create a new subscriber
	 * 
	 * @param subscriberName
	 *           the name of subscriber to be created
	 * @param endpoint
	 *           subscriber's end point that is listening online for message feed
	 * @param topics
	 *           a list of topics to subscribe to
	 * @return a list of topics that have been successfully subscribed
	 */
	@Override
	public Subscriber createSubscriber(String subscriberName, String endpoint,
			Collection<String> topics) {
		// at least one topic is required to subscribe
		if (null == topics || topics.size() == 0) {
			throw new InternalErrorException("No topic selected for subscriber: "
					+ subscriberName);
		}

		// endpoint must be valid and alive
		if (!WebClientTool.isEndpointAlive(endpoint)) {
			throw new InternalErrorException(
					"Failed subscription due to inaccessible endpoint: " + endpoint);
		}

		try {
			// set subscriber ID
			String subscriberId = UUID.randomUUID().toString();

			// set meta data for the subscriber
			Map<String, Object> subscriberProp = new HashMap<String, Object>();
			subscriberProp.put("subscriberName", subscriberName);
			subscriberProp.put("endpoint", endpoint);

			// create an non-exclusive, non-autodelete, durable queue named by the
			// subscriber ID
			final Channel channel = connection.createChannel();
			channel.queueDeclare(subscriberId, true, false, false, subscriberProp);
			// bind the subscriber queue to the exchanges of the target topics
			Collection<String> topicsSubscribed = subscribeTopicHelper(
					subscriberId, topics);

			// set a callback object for feed upon message received
			final SubscriptionFeeder feeder = new SubscriptionFeeder(
					subscriberName, subscriberId, endpoint);

			// start consuming messages from the topic and feed to the subscriber
			DefaultConsumer consumer = new DefaultConsumer(channel) {
				// Implement consumer callback so the operation gets dispatched to
				// background using thread pool managed by RabbitMQ Client
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope,
						BasicProperties properties, byte[] body) throws IOException {
					long deliveryTag = envelope.getDeliveryTag();
					String messageBody = new String(body);
					String messageId = properties.getMessageId();
					String topicName = properties.getHeaders().get("topic")
							.toString();
					Message message = new Message(messageId, topicName, body);

					LOG.debug("Message received for subscriber [Message ID: "
							+ messageId + "]:");
					LOG.debug(messageBody);

					boolean success = true;
					try {
						feeder.feed(message);
					} catch (Exception e) {
						LOG.error(e,
								"Failed on initial feed. Will retry delivery shortly.");
						success = false;
					}

					if (success) {
						channel.basicAck(deliveryTag, false);
						LOG.debug("Successfully feed and Ack sent for message "
								+ messageId);
					} else {
						// if the initial feed failed, start redelivery attempts
						// according to the retry policy
						int numRetry = 0;
						int initInterval = defaultRetryPolicy
								.getInitialRetryInterval();
						int maxRetryNum = defaultRetryPolicy.getMaxRetryNum();
						try {
							// Phase 1, initial retry for configured number of times
							for (int i = 0; i < defaultRetryPolicy
									.getInitialRetryNum(); i++) {
								try {
									feeder.feed(message);
									success = true;
								} catch (Exception e) {
									LOG.debug("Cannot feed the message. Initial retry #"
											+ numRetry);
									success = false;
								}

								if (success) {
									break;
								} else {
									LOG.debug("Sleeping for " + initInterval
											+ " before retry.");
									Thread.sleep(initInterval);
								}
								numRetry++;
							}

							// Phase 2, if still failing, retry until reaching the
							// configured max
							// number of retries with incremental time interval
							if (!success) {
								while (numRetry < maxRetryNum) {
									try {
										feeder.feed(message);
										success = true;
									} catch (Exception e) {
										LOG.debug("Cannot feed the message. Retry #"
												+ numRetry);
										success = false;
									}

									if (success) {
										break;
									} else {
										LOG.debug("Sleeping for " + initInterval
												+ " before retry.");
										Thread.sleep(initInterval);
										initInterval += 1000;
									}
									numRetry++;
								}
							}

							// Phase 3, if still failing, stop feeding and discard the
							// message
							if (!success) {
								channel.basicReject(deliveryTag, false);
								LOG.debug("Sent BasicRejeck to discard the undelivarable message: "
										+ messageId);
							} else {
								channel.basicAck(deliveryTag, false);
								LOG.debug("Successfully feed and Ack sent for message: "
										+ messageId);
							}
						} catch (Exception e) {
							LOG.error(e, "Error redelivering message.");
						}
					}
				}
			};

			LOG.debug("Start consuming messages for Subscriber " + subscriberId + " (" + subscriberName + ")");
			// autoAck = false so consumer explicitly sends ack
			channel.basicConsume(subscriberId, false, consumer);

			return new Subscriber(subscriberId, subscriberName, endpoint,
					topicsSubscribed);
		} catch (Exception e) {
			LOG.error(e, "Error creating subscriber. Subscriber: "
					+ subscriberName);
			throw new InternalErrorException(
					"Error creating subscriber. Subscriber: " + subscriberName);
		}
	}

	/**
	 * Bind a subscriber to a list of topics
	 * 
	 * @param subscriberId
	 *           the ID of subscriber
	 * @param topics
	 *           a list of topics to which the subscriber suppose to subscribe
	 * @return a list of topics that have been successfully subscribed, excluding
	 *         any topics that failed to subscribe
	 */
	private Collection<String> subscribeTopicHelper(String subscriberId,
			Collection<String> topics) {
		try {
			Collection<String> topicsSubscribed = new HashSet<String>();
			Channel tmpChannel = connection.createChannel();

			// for each topic to subscribe, bind the subscriber queue to the
			// topic's exchange
			for (String topic : topics) {
				try {
					// TODO: limit max number of subscriptions for a topic ?
					tmpChannel.queueBind(subscriberId, topic, DEFAULT_ROUTING_KEY);
					// collect topics of successful subscription
					topicsSubscribed.add(topic);
				} catch (IOException e) {
					// skip in case any topic cannot be subscribed for whatever
					// reason
					LOG.error(e, "Failed subscribing Subscriber " + subscriberId
							+ ") to Topic " + topic);
					continue;
				}
			}
			tmpChannel.close();
			LOG.debug("Successfully subscribed topics: " + topicsSubscribed);

			return topicsSubscribed;
		} catch (Exception e) {
			LOG.error(e, "Failed on subscription. Subscriber: " + subscriberId);
			throw new InternalErrorException("Failed on subscription. subscriber: " + subscriberId);
		}
	}

	/**
	 * Subscribe an existing subscriber to more topics
	 * 
	 * @param subscriberId
	 *           the ID of subscriber
	 * @param topics
	 *           a list of topics to subscribe to
	 * @return a list of topics that have been successfully subscribed
	 */
	@Override
	public Collection<String> subscribeTopic(String subscriberId,
			Collection<String> topics) {
		if (null == topics || topics.size() == 0) {
			throw new InternalErrorException("No topic selected for subscriber: " + subscriberId);
		}

		// subscribe to the requested topics
		Collection<String> topicsSubscribed = subscribeTopicHelper(subscriberId, topics);
		return topicsSubscribed;
	}

	@Override
	public Subscriber getSubscriberInfo(String subscriberId) {
		try {
			// URI: api/queues/vhost/queue/bindings
			String uri = RABBITMQ_WEBAPI +
								"queues/" +
								(RABBITMQ_VHOST.equals("/") ? "%2f" : RABBITMQ_VHOST) +
								"/" +
								subscriberId +
								"/bindings";
			WebResource webResource = this.webclient.resource(uri);
			ClientResponse response = webResource.accept("application/json").get(
					ClientResponse.class);

			int statusCode = response.getStatus();
			if (statusCode != 200) {
				LOG.error("Error getting subscriber's topics from RMQ server. Status code: " + statusCode);
				throw new InternalErrorException("Error describing Subscriber " + subscriberId);
			}

			Collection<String> subscribedTopics = JsonProcessor.parseJsonList(
					response.getEntity(String.class), "source", null);
			LOG.debug("Topic list retrieved from RMQ for Subscriber {}: {}", subscriberId, subscribedTopics.toString());

			// Get subscriber name and endpoint
			uri = RABBITMQ_WEBAPI +
					"queues/" +
					(RABBITMQ_VHOST.equals("/") ? "%2f" : RABBITMQ_VHOST) +
					"/" +
					subscriberId;
			webResource = this.webclient.resource(uri);
			response = webResource.accept("application/json").get(ClientResponse.class);
			statusCode = response.getStatus();
			if (statusCode != 200) {
				LOG.error("Error getting subscriber's properties from RMQ server. Status code: " + statusCode);
				throw new InternalErrorException("Error describing Subscriber " + subscriberId);
			}

			Collection<String> keysToSearch = new ArrayList<String>();
			keysToSearch.add("subscriberName");
			keysToSearch.add("endpoint");
			Map<String, String> subscriberProp = JsonProcessor.parseJsonEntities(
					response.getEntity(String.class), keysToSearch);
			LOG.debug(
					"Subscriber name and endpoint retrieved from RMQ for Subscriber {}: {}",
					subscriberId, subscriberProp.toString());

			return new Subscriber(subscriberId,
											subscriberProp.get("subscriberName"),
											subscriberProp.get("endpoint"),
											subscribedTopics);
		} catch (Exception e) {
			LOG.error(e, "Error describing Subscriber {}.", subscriberId);
			throw new InternalErrorException("Error describing Subscriber " + subscriberId);
		}
	}

	/**
	 * Delete an existing subscriber along with its subscriptions
	 * 
	 * @param subscriberId
	 *           the ID of subscriber
	 * @param topics
	 *           a list of topics to subscribe to
	 * @return true if success
	 */
	@Override
	public boolean deleteSubscriber(String subscriberId) {
		try {
			Channel tmpChannel = connection.createChannel();
			tmpChannel.queueDelete(subscriberId);
			tmpChannel.close();
		} catch (Exception e) {
			LOG.error(e, "Failed deleting Subscriber " + subscriberId);
			throw new InternalErrorException("Failed deleting Subscriber " + subscriberId);
		}
		return true;
	}

	/**
	 * Un-subscribe an existing subscriber out of a list of topics
	 * 
	 * @param subscriberId
	 *           the ID of subscriber
	 * @param topics
	 *           a list of topics to subscribe to
	 * @return a list of topics that have been successfully un-subscribed
	 */
	@Override
	public Collection<String> unsubscribeTopic(String subscriberId, Collection<String> topics) {
		if (null == topics || topics.size() == 0) {
			throw new InternalErrorException("No topic selected for subscriber: " + subscriberId);
		}

		LOG.debug("Expect to unsubscribe topics " + topics);
		Collection<String> topicsUnsubscribed = new HashSet<String>();
		try {
			Channel tmpChannel = connection.createChannel();
			// unbind the subscriber queue from each topic requested
			for (String topic : topics) {
				try {
					tmpChannel.queueUnbind(subscriberId, topic, DEFAULT_ROUTING_KEY);
					LOG.debug("Unsubscribed Subscriber " + subscriberId + " from Topic " + topic);
				} catch (Exception e) {
					LOG.error(e, "Failed unsubscribing Subscriber " + subscriberId + " from Topic " + topic);
					continue;
				}
				topicsUnsubscribed.add(topic);
			}
			tmpChannel.close();
		} catch (Exception e) {
			LOG.error(e, "Error unsubscribing Subscriber " + subscriberId);
			throw new InternalErrorException("Error unsubscribing Subscriber " + subscriberId);
		}
		return topicsUnsubscribed;
	}

}
