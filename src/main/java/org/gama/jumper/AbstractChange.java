package org.gama.jumper;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractChange implements Change {
	
	private final UpdateId updateId;
	private final boolean shouldAlwaysRun;
	private final Statistics executionStatistics;
	
	public AbstractChange(UpdateId updateId, boolean shouldAlwaysRun) {
		this.updateId = updateId;
		this.shouldAlwaysRun = shouldAlwaysRun;
		executionStatistics = new Statistics();
	}
	
	public AbstractChange(String identifier, boolean shouldAlwaysRun) {
		this(new UpdateId(identifier), shouldAlwaysRun);
	}
	
	@Override
	public UpdateId getIdentifier() {
		return updateId;
	}
	
	@Override
	public boolean shouldAlwaysRun() {
		return shouldAlwaysRun;
	}
	
	public Statistics getExecutionStatistics() {
		return executionStatistics;
	}
}
