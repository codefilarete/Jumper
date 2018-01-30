package org.gama.jumper.impl;

import org.gama.jumper.NoopExecutionListener;
import org.gama.jumper.Statistics;
import org.gama.jumper.Change;
import org.gama.lang.trace.Chrono;

/**
 * An {@link org.gama.jumper.ExecutionListener} to fullfill statistics
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
