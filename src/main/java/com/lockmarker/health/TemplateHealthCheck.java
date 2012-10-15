package com.lockmarker.health;

import com.yammer.metrics.core.HealthCheck;

public class TemplateHealthCheck extends HealthCheck {
	private final String template;
	
    public TemplateHealthCheck(String template) {
        super("template");
        this.template = template;
    }

    @Override
    protected Result check() throws Exception {
        final String test = String.format(template, "TEST");
        if (!test.contains("TEST")) {
            return Result.unhealthy("template doesn't include a name");
        }
        return Result.healthy();
    }
}
