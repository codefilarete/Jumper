package org.codefilarete.jumper.impl;

import org.codefilarete.jumper.NoopExecutionListener;
import org.codefilarete.jumper.Statistics;
import org.codefilarete.jumper.Change;
import org.codefilarete.tool.trace.Chrono;

/**
 * An {@link org.codefilarete.jumper.ExecutionListener} to fullfill statistics
 * 
 * @author Guillaume Mary
 */
public class StatisicsExecutionListener extends NoopExecutionListener {
	
	private Chrono chrono = new Chrono();
	
	private final ApplicationUpdateStatistics applicationUpdateStatistics;
	
	public StatisicsExecutionListener() {
		applicationUpdateStatistics = new ApplicationUpdateStatistics();
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
		applicationUpdateStatistics.setExecutionStatistics(change.getIdentifier(), statistics);
	}
}
