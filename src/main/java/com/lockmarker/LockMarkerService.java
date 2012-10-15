package com.lockmarker;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.lockmarker.admin.ServiceShutdownTask;
import com.lockmarker.api.application.MessagingDispatcher;
import com.lockmarker.api.application.rabbitmq.RabbitMQDispatcher;
import com.lockmarker.config.MessagingConfiguration;
import com.lockmarker.health.TemplateHealthCheck;
import com.lockmarker.resources.LockMarkerResource;
import com.lockmarker.resources.FeedListenerResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;


public class LockMarkerService extends Service<MessagingConfiguration> {
	private MessagingDispatcher dispatcher;

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new AbstractModule() {
			@Override
			protected void configure() {
				// default dispatcher is NopMessagingDispatcher. To override it, 
				// pass java command line arg -Ddispatcher=<NewDispatcher>
				// e.g. java -Ddispatcher=Rabbit -jar msgas-0.0.1.jar server msgas.yml
				String dispatcherClassName = System.getProperty("dispatcher");
				Class<? extends MessagingDispatcher> dispatcherClass = RabbitMQDispatcher.class;
				if ((dispatcherClassName != null) && 
					(dispatcherClassName.indexOf("Rabbit") != -1)) {
					dispatcherClass = RabbitMQDispatcher.class;
				}
				bind(MessagingDispatcher.class).to(dispatcherClass);
			}
		});
		injector.getInstance(LockMarkerService.class).run(args);
	}

	@Inject
	private LockMarkerService(MessagingDispatcher dispatcher) {
		super("messaging-as-a-service");
		this.dispatcher = dispatcher;
	}

	@Override
	protected void initialize(MessagingConfiguration configuration,
			                  Environment environment) {
		try {
			final String template = configuration.getTemplate();
			environment.addHealthCheck(new TemplateHealthCheck(template));
            dispatcher.loadConfiguration(configuration);
            environment.addResource(new LockMarkerResource(dispatcher));
            environment.addResource(new FeedListenerResource());
			environment.addTask(new ServiceShutdownTask());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
