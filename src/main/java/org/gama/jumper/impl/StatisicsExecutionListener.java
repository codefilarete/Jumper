package org.gama.jumper.impl;

import org.gama.jumper.NoopExecutionListener;
import org.gama.jumper.Statistics;
import org.gama.jumper.Update;
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
	public void beforeRun(Update update) {
		chrono.start();
	}
	
	@Override
	public void afterRun(Update update) {
		long elapsedTime = chrono.getElapsedTime();
		Statistics statistics = new Statistics();
		statistics.setExecutionTime(elapsedTime);
		applicationUpdateStatistics.setExecutionStatistics(update.getIdentifier(), statistics);
	}
}
