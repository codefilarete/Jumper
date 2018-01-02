package org.gama.jumper;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractUpdate implements Update {
	
	private final UpdateId updateId;
	private final boolean shouldAlwaysRun;
	
	public AbstractUpdate(UpdateId updateId, boolean shouldAlwaysRun) {
		this.updateId = updateId;
		this.shouldAlwaysRun = shouldAlwaysRun;
	}
	
	public AbstractUpdate(String identifier, boolean shouldAlwaysRun) {
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
}
