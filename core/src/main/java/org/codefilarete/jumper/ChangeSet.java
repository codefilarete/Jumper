package org.codefilarete.jumper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public class ChangeSet {
	
	private final ChangeId changeId;
	private final boolean shouldAlwaysRun;
	private final List<Change> changes = new ArrayList<>();
	
	public ChangeSet(ChangeId changeId) {
		this(changeId, false);
	}
	
	public ChangeSet(String identifier) {
		this(identifier, false);
	}
	
	public ChangeSet(ChangeId changeId, boolean shouldAlwaysRun) {
		this.changeId = changeId;
		this.shouldAlwaysRun = shouldAlwaysRun;
	}
	
	public ChangeSet(String identifier, boolean shouldAlwaysRun) {
		this(new ChangeId(identifier), shouldAlwaysRun);
	}
	
	public ChangeId getIdentifier() {
		return changeId;
	}
	
	public List<Change> getChanges() {
		return changes;
	}
	
	public ChangeSet addChanges(Change... changes) {
		this.changes.addAll(Arrays.asList(changes));
		return this;
	}
	
	/**
	 * Indicates if this {@link ChangeSet} must be executed even if it was already ran. Default is no (false).
	 * May be overridden according to task.
	 */
	public boolean alwaysRun() {
		return shouldAlwaysRun;
	}
	
	/**
	 * Indicates if this {@link ChangeSet} must be run on the given {@link Context}. Default is yes (true).
	 * Should be overridden to implement a conditional reason of execution according to {@link Context}.
	 */
	protected boolean shouldRun(Context context) {
		return true;
	}
	
	/**
	 * Interface for {@link ChangeSet}s that are signed with a MD5, SHA1, or whatever.
	 * Aimed at being used to check if this {@link ChangeSet} has changed since previous execution. So storage must record signature.
	 */
	Set<Checksum> getCompatibleChecksums() {
		return Collections.EMPTY_SET;
	}
}
