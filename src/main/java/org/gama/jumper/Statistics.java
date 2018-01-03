package org.gama.jumper;

/**
 * A basic statistics container to help to monitor an update
 * 
 * @author Guillaume Mary
 */
public class Statistics {
	
	private long executionTime;
	
	public Statistics() {
	}
	
	public long getExecutionTime() {
		return executionTime;
	}
	
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}
}
