package com.lockmarker.config;

import com.yammer.dropwizard.config.Configuration;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MessagingConfiguration extends Configuration {
	@NotNull
	@JsonProperty
	private boolean useMiddleware;
	
	@NotNull
	@JsonProperty
	private boolean authenticationEnabled;
	
    @NotEmpty
    @JsonProperty
    private String template;
    
    @Valid
    @NotNull
    @JsonProperty
    private RabbitMQConfiguration rabbitmq = new RabbitMQConfiguration();
    
    public String getTemplate() {
        return template;
    }
    
    public RabbitMQConfiguration getRabbitMQConfiguration() {
        return rabbitmq;
    }
    
	public boolean getUseMiddleware() {
		return useMiddleware;
	}

	public void setUseMiddleware(boolean useMiddleware) {
		this.useMiddleware = useMiddleware;
	}
	
	public boolean getAuthenticationEnabled() {
		return authenticationEnabled;
	}
	
	public void setAuthenticationEnabled(boolean authenticationEnabled) {
		this.authenticationEnabled = authenticationEnabled;
	}
}