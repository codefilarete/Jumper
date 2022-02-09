package org.codefilarete.jumper;

/**
 * Exception dedicated to execution of updates.
 * 
 * @author Guillaume Mary
 */
public class ExecutionException extends RuntimeException {
	
	public ExecutionException(String message) {
		super(message);
	}
	
	public ExecutionException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public ExecutionException(Throwable cause) {
		super(cause);
	}
}
