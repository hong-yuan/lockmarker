package com.lockmarker.admin;

import java.io.PrintWriter;
import com.google.common.collect.ImmutableMultimap;
import com.yammer.dropwizard.tasks.Task;

/**
 * Perform an administrative task to shutdown the whole service
 */
public class ServiceShutdownTask extends Task {
	private final Runtime runtime;
	
	/**
	 * Create a new {@link ServiceShutdownTask}.
	 */
    public ServiceShutdownTask()  {
    	this(Runtime.getRuntime());
    }
    
    /**
     * Creates a new {@link ServiceShutdownTask} with the given {@link Runtime} instance.
     * <p/>
     * <b>Use {@link ServiceShutdownTask#ServiceShutdownTask()} instead.</b>
     *
     * @param runtime    a {@link Runtime} instance
     */
    public ServiceShutdownTask(Runtime runtime) {
        super("shutdown");
        this.runtime = runtime;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) {
        output.println("Shutting down Messaging Service...");
        output.flush();
        runtime.exit(0);
    }
}
