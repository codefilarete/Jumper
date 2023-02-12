package org.codefilarete.jumper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Guillaume Mary
 */
public class ChangeSet {
	
	public static ChangeSet changeSet(String changeSetId, ChangeBuilder... changes) {
		return new ChangeSet(changeSetId).addChanges(changes);
	}
	
	private final ChangeSetId changeSetId;
	private boolean shouldAlwaysRun = false;
	private final List<Change> changes = new ArrayList<>();
	
	public ChangeSet(ChangeSetId changeSetId) {
		this.changeSetId = changeSetId;
	}
	
	public ChangeSet(String identifier) {
		this(new ChangeSetId(identifier));
	}
	
	public ChangeSetId getIdentifier() {
		return changeSetId;
	}
	
	public List<Change> getChanges() {
		return changes;
	}
	
	public ChangeSet addChanges(Change... changes) {
		this.changes.addAll(Arrays.asList(changes));
		return this;
	}
	
	public ChangeSet addChanges(ChangeBuilder... changes) {
		this.changes.addAll(Arrays.stream(changes).map(ChangeBuilder::build).collect(Collectors.toList()));
		return this;
	}
	
	/**
	 * Indicates if this {@link ChangeSet} must be executed even if it was already ran. Default is no (false).
	 */
	public boolean shouldAlwaysRun() {
		return shouldAlwaysRun;
	}
	
	/**
	 * Marks this {@link ChangeSet} to be executed even if it was already ran or not.
	 *
	 * @param yeOrNo true for changeSet to be executed even if it was already ran, false to run only once.
	 */
	public ChangeSet alwaysRun(boolean yeOrNo) {
		shouldAlwaysRun = yeOrNo;
		return this;
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
	
	public interface ChangeBuilder {
		
		Change build();
	}
}
