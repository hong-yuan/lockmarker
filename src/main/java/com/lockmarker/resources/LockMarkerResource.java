package com.lockmarker.resources;

import com.lockmarker.api.exceptions.*;

import com.yammer.dropwizard.logging.Log;
import com.yammer.metrics.annotation.Timed;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;

@Path("/v1.0")
@Produces(MediaType.APPLICATION_JSON)
public class LockMarkerResource {
	private static final Log			LOG	= Log.forClass(LockMarkerResource.class);
	protected static JsonNodeFactory	fact	= JsonNodeFactory.instance;

	public LockMarkerResource() {
	}

	@Path("/ping")
	@GET
	@Timed
	public Response ping() {
		try {
			final Status status = Status.OK;
			ObjectNode result = fact.objectNode();
			result.put("status", status.getStatusCode());
			result.put("message", "pong");
			return buildResponse(LockMarkerResource.class, status, result);
		} catch (AuthenticationException authex) {
			return generateAuthenticationErrorResponse(authex);
		} catch (Exception e) {
			throw handleException(e);
		}
	}

	private RuntimeException handleException(Exception ex) {
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

	private Response buildResponse(Class<?> resource, Status status, Object entity) {
		Response.ResponseBuilder builder = Response.created(UriBuilder.fromResource(resource).build());
		builder.status(status).entity(entity);
		Response response = builder.build();
		LOG.debug("Resposne sent: {}", response);
		return response;
	}

	private Response generateAuthenticationErrorResponse(AuthenticationException authex) {
		final Status status = Status.UNAUTHORIZED;
		ObjectNode result = fact.objectNode();
		result.put("status", status.getStatusCode());
		ObjectNode details = fact.objectNode();
		details.put("message", authex.getMessage());
		details.put("moreInfo", "https://wiki.hpcloud.net/display/paas/Messaging+Service+Authentication");
		result.put("details", details);
		return buildResponse(LockMarkerResource.class, status, result);
	}
}
