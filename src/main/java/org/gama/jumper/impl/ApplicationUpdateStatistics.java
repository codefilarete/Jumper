package org.gama.jumper.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.gama.jumper.Statistics;
import org.gama.jumper.ChangeId;

/**
 * @author Guillaume Mary
 */
public class ApplicationUpdateStatistics {
	
	/** {@link LinkedHashMap} to keep execution order */
	private final Map<ChangeId, Statistics> executionStatistics = new LinkedHashMap<>();
	
	public void setExecutionStatistics(ChangeId changeId, Statistics statistics) {
		executionStatistics.put(changeId, statistics);
	}
	
	public long getTotalTime() {
		return executionStatistics.values().stream().mapToLong(Statistics::getExecutionTime).sum();
	}
}
