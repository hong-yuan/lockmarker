package com.lockmarker;

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
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;

public class LockMarkerService extends Service<MessagingConfiguration> {

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
		super("LockMarker Service");
	}

	@Override
	protected void initialize(MessagingConfiguration configuration,
			                  Environment environment) {
		try {
			final String template = configuration.getTemplate();
			environment.addHealthCheck(new TemplateHealthCheck(template));
            environment.addResource(new LockMarkerResource());
			environment.addTask(new ServiceShutdownTask());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
