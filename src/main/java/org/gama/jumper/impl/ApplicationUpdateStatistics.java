package org.gama.jumper.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.gama.jumper.Statistics;
import org.gama.jumper.UpdateId;

/**
 * @author Guillaume Mary
 */
public class ApplicationUpdateStatistics {
	
	/** {@link LinkedHashMap} to keep execution order */
	private final Map<UpdateId, Statistics> executionStatistics = new LinkedHashMap<>();
	
	public void setExecutionStatistics(UpdateId updateId, Statistics statistics) {
		executionStatistics.put(updateId, statistics);
	}
	
	public long getTotalTime() {
		return executionStatistics.values().stream().mapToLong(Statistics::getExecutionTime).sum();
	}
}
