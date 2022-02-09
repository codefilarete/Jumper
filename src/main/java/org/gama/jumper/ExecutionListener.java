package org.codefilarete.jumper;

/**
 * A simple contract to listen to an execution update.
 * 
 * @author Guillaume Mary
 */
public interface ExecutionListener {
	
	/**
	 * Is called before the execution of the given change
	 * @param change the change that's going to be executed
	 */
	void beforeRun(Change change);
	
	/**
	 * Is called after the execution of the given change
	 * @param change the executed change
	 */
	void afterRun(Change change);
}
