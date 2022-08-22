package org.codefilarete.jumper.impl;

import java.time.Duration;
import java.time.Instant;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.ChangeSetRunner;
import org.codefilarete.jumper.ChangeSetExecutionListener.FineGrainExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listener of change execution that provide some logs about it :
 * - execution time per change
 * - total execution time
 *
 * @author Guillaume Mary
 */
public class LoggingExecutionListener implements FineGrainExecutionListener {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ChangeSetRunner.class);
	
	private final Logger logger;
	
	private Instant beginAllInstant;
	
	private Instant beginChangeInstant;
	
	public LoggingExecutionListener() {
		this(LOGGER);
	}
	
	public LoggingExecutionListener(Logger logger) {
		this.logger = logger;
	}
	
	@Override
	public void beforeRun(Change change) {
		logger.debug("Executing {}", change.getIdentifier());
		beginChangeInstant = Instant.now();
	}
	
	@Override
	public void afterRun(Change change) {
		Duration duration = Duration.between(beginChangeInstant, Instant.now());
		logger.debug("Execution took {}", String.format("%sm %ss %sms", duration.toMinutes(), duration.getSeconds(), duration.toMillis()));
	}
	
	@Override
	public void beforeProcess() {
		beginAllInstant = Instant.now();
	}
	
	@Override
	public void afterProcess() {
		Duration duration = Duration.between(beginAllInstant, Instant.now());
		logger.debug("Total execution took {}", String.format("%sm %ss %sms", duration.toMinutes(), duration.getSeconds(), duration.toMillis()));
	}
	
	@Override
	public void afterRun(String statement, Long updatedRowCount) {
		if (updatedRowCount != null) {
			logger.trace("Updated row count {}", updatedRowCount);
		}
	}
}
