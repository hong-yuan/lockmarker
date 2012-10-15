package com.lockmarker.resources;

import com.lockmarker.api.application.MessagingDispatcher;
import com.lockmarker.api.application.model.Message;
import com.lockmarker.api.application.model.Subscriber;
import com.lockmarker.api.exceptions.*;
import com.lockmarker.utils.JsonProcessor;

import com.yammer.dropwizard.logging.Log;
import com.yammer.metrics.annotation.Timed;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.util.Collection;

@Path("/v1.0/{tenant_id}")
@Produces(MediaType.APPLICATION_JSON)
public class LockMarkerResource {
	private static final Log			LOG	= Log.forClass(LockMarkerResource.class);
	protected static JsonNodeFactory	fact	= JsonNodeFactory.instance;
	private MessagingDispatcher		dispatcher;

	public LockMarkerResource(MessagingDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	@Path("/topic")
	@GET
	@Timed
	public Response listTopics(@PathParam("tenant_id") String tenantIdFromUri) {
		try {
			Collection<String> topics = dispatcher.getTopics(tenantIdFromUri);
			final Status status = Status.OK;
			ArrayNode resultArray = fact.arrayNode();
			for (String t : topics) {
				resultArray.add(t);
			}
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("topics", resultArray);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/topic/{topicName}")
	@GET
	@Timed
	public Response describeTopic(@PathParam("topicName") String topicName,
											@PathParam("tenant_id") String tenantIdFromUri) {
		try {
			Collection<String> topicInfo = dispatcher.describeTopic(tenantIdFromUri, topicName);
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ArrayNode subscriberList = fact.arrayNode();
			for (String s : topicInfo) {
				subscriberList.add(s);
			}
			result.put("topic", topicName);
			result.put("subscribers", subscriberList);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (MsgasException msgex) {
			final Status status = Status.NOT_FOUND;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-describetopic");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/topic")
	@POST
	@Timed
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createTopic(ObjectNode request,
			                      @PathParam("tenant_id") String tenantIdFromUri) {
		try {
			String topicName = JsonProcessor.jgetString(request, "name");
			dispatcher.createTopic(tenantIdFromUri, topicName);
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("topic", topicName);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (MsgasException msgex) {
			final Status status = Status.CONFLICT;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-createtopic");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/topic/{id}")
	@DELETE
	@Timed
	public Response deleteTopic(@PathParam("id") String topicName,
			                      @PathParam("tenant_id") String tenantIdFromUri) {
		try {
			dispatcher.deleteTopic(tenantIdFromUri, topicName);
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("topic", topicName);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (MsgasException msgex) {
			final Status status = Status.NOT_FOUND;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("topic", topicName);
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-deletetopic");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/topic/{topicName}")
	@POST
	@Timed
	@Consumes(MediaType.APPLICATION_JSON)
	public Response messageCommands(@Valid ObjectNode request,
			                          @PathParam("topicName") String topicName,
			                          @PathParam("tenant_id") String tenantIdFromUri) {
		Status status;
		ObjectNode result = fact.objectNode();
		try {
			String command = JsonProcessor.jgetString(request, "command");
			if (command.equals("send")) {
				String message = JsonProcessor.jgetString(request, "message");
				String msgId = dispatcher.sendMessage(tenantIdFromUri, topicName, message);
				status = Status.OK;
				result.put("status", status.getStatusCode());
				result.put("messageId", msgId);
			} else if (command.equals("receive")) {
				Message message = dispatcher.pullMessage(tenantIdFromUri, topicName);
				if (null == message) {
					status = Status.NO_CONTENT;
					result = null;
				} else {
					status = Status.OK;
					result.put("status", status.getStatusCode());
					result.put("messageId", message.getId());
					result.put("topicName", message.getTopic());
					result.put("message", new String(message.getBody()));
				}
			} else if (command.equals("delete")) {
				String messageId = JsonProcessor.jgetString(request, "messageId");
				dispatcher.deleteMessage(tenantIdFromUri, topicName, messageId);
				status = Status.OK;
				result.put("status", status.getStatusCode());
				result.put("messageId", messageId);
			} else {
				throw new IllegalArgumentException("Unrecognized command");
			}
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (IllegalArgumentException iaex) {
			status = Status.BAD_REQUEST;
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", "Invalid command or parameters for topic " + topicName);
			details.put("moreInfo", "https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (MsgasException msgex) {
			status = Status.INTERNAL_SERVER_ERROR;
			result.put("status", status.getStatusCode());
			result.put("topic", topicName);
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details
					.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-deletetopic");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/subscriber")
	@PUT
	@Timed
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createSubscriber(ObjectNode request,
												@PathParam("tenant_id") String tenantIdFromUri) {
		try {
			String subscriberName = JsonProcessor.jgetString(request, "subscriber");
			String endpoint = JsonProcessor.jgetString(request, "endpoint");
			Collection<String> topics = JsonProcessor.jgetChildrenInString(request, "topics");
			assert (subscriberName != null
						&& endpoint != null && topics.size() > 0);
			Subscriber sub = dispatcher.createSubscriber(subscriberName, endpoint, topics);
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("subscriberId", sub.getId());
			ArrayNode subscriptions = result.putArray("subscribedTo");
			for (String topic : sub.getTopics()) {
				subscriptions.add(topic);
			}
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (IllegalArgumentException iaex) {
			final Status status = Status.BAD_REQUEST;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", "Malformed subscription request");
			details.put("moreInfo",
					"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-push");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (MsgasException msgex) {
			final Status status = Status.INTERNAL_SERVER_ERROR;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details
					.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-subscribe");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/subscriber/{subscriberId}")
	@GET
	@Timed
	public Response describeSubscriber(@PathParam("subscriberId") String subscriberId,
			                             @PathParam("tenant_id") String tenantIdFromUri) {
		try {
			Subscriber subscriberInfoInfo = dispatcher.getSubscriberInfo(subscriberId);
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("id", subscriberInfoInfo.getId());
			result.put("name", subscriberInfoInfo.getName());
			result.put("endpoint", subscriberInfoInfo.getEndpoint());
			ArrayNode subscriptions = result.putArray("subscribedTo");
			Collection<String> topics = subscriberInfoInfo.getTopics();
			for (String t : topics) {
				subscriptions.add(t);
			}
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (MsgasException msgex) {
			final Status status = Status.INTERNAL_SERVER_ERROR;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details
					.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-describesubscriber");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	@Path("/subscriber/{id}")
	@DELETE
	@Timed
	public Response deleteSubscriber(@PathParam("id") String subscriberId,
												@PathParam("tenant_id") String tenantIdFromUri) {
		try {
			dispatcher.deleteSubscriber(subscriberId);
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("subscriberId", subscriberId);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (MsgasException msgex) {
			final Status status = Status.INTERNAL_SERVER_ERROR;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			ObjectNode details = fact.objectNode();
			details.put("message", msgex.getMessage());
			details.put("moreInfo",
							"https://wiki.hpcloud.net/display/paas/Messaging+Service+Operations#MessagingServiceOperations-deletesubscriber");
			result.put("details", details);
			return buildResponse(LockMarkerResource.class, status, result, tenantIdFromUri);
		} catch (Exception ex) {
			throw handleException(ex, tenantIdFromUri);
		}
	}

	private RuntimeException handleException(Exception ex, String tenantId) {
		Response.Status status;
		StringBuilder sb = new StringBuilder();
		if (ex instanceof TopicNotFoundException) {
			sb.append("Topic ").append(ex.getMessage()).append(" does not exist.");
			status = Status.NOT_FOUND;
		} else if (ex instanceof MessageNotFoundException) {
			sb.append("Message ").append(ex.getMessage()).append(" does not exist.");
			status = Status.NOT_FOUND;
		} else if (ex instanceof IllegalArgumentException) {
			sb.append("Illegal Argument ").append(ex.getMessage());
			status = Status.BAD_REQUEST;
		} else if (ex instanceof TopicExistsException) {
			sb.append("Topic already exists ").append(ex.getMessage());
			status = Status.CONFLICT;
		} else {
			status = Status.INTERNAL_SERVER_ERROR;
			sb.append("Exception ");
			sb.append(ex.getLocalizedMessage());
			sb.append("\nThread ");
			sb.append(Thread.currentThread());
			sb.append('\n');
			for (StackTraceElement element : ex.getStackTrace()) {
				sb.append(element.toString());
				sb.append('\n');
			}
		}
		Response response = Response.status(status).entity(sb.toString()).type("text/plain").build();
		return new WebApplicationException(response);
	}

	private Response buildResponse(Class<?> resource, Status status, Object entity, String tenantId) {
		Response.ResponseBuilder builder = Response.created(UriBuilder.fromResource(resource).build());
		builder.status(status).entity(entity);
		return builder.build();
	}

	private Response generateAuthenticationErrorResponse(AuthenticationException authex) {
		final Status status = Status.UNAUTHORIZED;
		ObjectNode result = fact.objectNode();
		result.put("status", status.getStatusCode());
		ObjectNode details = fact.objectNode();
		details.put("message", authex.getMessage());
		details.put("moreInfo", "https://wiki.hpcloud.net/display/paas/Messaging+Service+Authentication");
		result.put("details", details);
		return buildResponse(LockMarkerResource.class, status, result, "");
	}
}
