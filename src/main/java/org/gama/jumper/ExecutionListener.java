package org.gama.jumper;

/**
 * A simple contract to listen to an execution update.
 * 
 * @author Guillaume Mary
 */
public interface ExecutionListener {
	
	/**
	 * Is called before the execution of the given update
	 * @param update the update that's going to be executed
	 */
	void beforeRun(Update update);
	
	/**
	 * Is called after the execution of the given update
	 * @param update the executed update
	 */
	void afterRun(Update update);
}
