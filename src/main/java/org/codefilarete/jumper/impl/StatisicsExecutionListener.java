package org.codefilarete.jumper.impl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.codefilarete.jumper.ChangeId;
import org.codefilarete.jumper.NoopExecutionListener;
import org.codefilarete.jumper.Statistics;
import org.codefilarete.jumper.Change;
import org.codefilarete.tool.trace.Chrono;

/**
 * An {@link org.codefilarete.jumper.ExecutionListener} to fulfill statistics
 * 
 * @author Guillaume Mary
 */
public class StatisicsExecutionListener extends NoopExecutionListener {
	
	private Chrono chrono = new Chrono();
	
	/** {@link LinkedHashMap} to keep execution order */
	private final Map<ChangeId, Statistics> executionStatistics = new HashMap<>();
	
	public StatisicsExecutionListener() {
	}
	
	@Override
	public void beforeRun(Change change) {
		chrono.start();
	}
	
	@Override
	public void afterRun(Change change) {
		long elapsedTime = chrono.getElapsedTime();
		Statistics statistics = new Statistics();
		statistics.setExecutionTime(elapsedTime);
		executionStatistics.put(change.getIdentifier(), statistics);
	}
}
