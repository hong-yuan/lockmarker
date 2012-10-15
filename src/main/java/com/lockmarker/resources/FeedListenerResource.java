package com.lockmarker.resources;

import com.yammer.dropwizard.logging.Log;
import com.yammer.metrics.annotation.Timed;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

/**
 * This is resource to test the subscriber's end point
 * listening on subscription feed for test convenience
 * TODO: move this test URI resource to client code
 */
@Path("/listener")
public class FeedListenerResource {
	private static final Log LOG = Log.forClass(FeedListenerResource.class);
	
	public FeedListenerResource() {
	}

    @PUT
    @Timed
    public Response getFeed(String request) {
        try {
        	if (request != null && !request.isEmpty()) {
        		LOG.info("\n\n-------- feed received --------\n" + 
        				 request + 
        		    	 "\n-------- end of feed --------\n\n");
        	}
        	return Response.ok().build();
        } catch (Exception e) {
        	throw new WebApplicationException();
        }
    }
}
