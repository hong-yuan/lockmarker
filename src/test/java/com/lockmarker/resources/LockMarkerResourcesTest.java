package com.lockmarker.resources;

import com.yammer.dropwizard.testing.ResourceTest;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

/**
 * Unit tests on LockMarker Service resources
 */
public class LockMarkerResourcesTest extends ResourceTest {

    private static final String serviceEndpoint = "/v1.0/";
    private static final LockMarkerResource lockMarkerServer = new LockMarkerResource(); 


    @Override
    protected void setUpResources() {
        addResource(lockMarkerServer);
    }

    @Test
    public void testPingPong() throws Exception {
        ObjectNode response = JsonNodeFactory.instance.objectNode();
        response.put("status", 200);
        response.put("message", "pong");

        assertThat(
                "Test Ping Pong API",
                client().resource(serviceEndpoint + "ping")
                        .type("application/json").accept("application/json")
                        .get(ObjectNode.class), equalTo(response));
    }

}
