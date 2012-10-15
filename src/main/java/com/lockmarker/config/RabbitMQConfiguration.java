package com.lockmarker.config;

import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import javax.validation.constraints.Min;
import javax.validation.constraints.Max;

public class RabbitMQConfiguration {
	@NotEmpty
	@JsonProperty
	private String host = "localhost";

	@Min(1)
	@Max(65535)
	@JsonProperty
	private int port = 5672;

	@NotEmpty
	@JsonProperty
	private String user = "guest";

	@NotEmpty
	@JsonProperty
	private String password = "guest";

	@NotEmpty
	@JsonProperty
	private String vhost = "/";

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getVhost() {
		return vhost;
	}
}
