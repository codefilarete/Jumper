package org.gama.jumper;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractChange implements Change {
	
	private final ChangeId changeId;
	private final boolean shouldAlwaysRun;
	private final Statistics executionStatistics;
	
	public AbstractChange(ChangeId changeId, boolean shouldAlwaysRun) {
		this.changeId = changeId;
		this.shouldAlwaysRun = shouldAlwaysRun;
		executionStatistics = new Statistics();
	}
	
	public AbstractChange(String identifier, boolean shouldAlwaysRun) {
		this(new ChangeId(identifier), shouldAlwaysRun);
	}
	
	@Override
	public ChangeId getIdentifier() {
		return changeId;
	}
	
	@Override
	public boolean shouldAlwaysRun() {
		return shouldAlwaysRun;
	}
	
	public Statistics getExecutionStatistics() {
		return executionStatistics;
	}
}
