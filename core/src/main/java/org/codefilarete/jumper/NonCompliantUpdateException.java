package org.codefilarete.jumper;

import java.util.Map;

/**
 * Exception dedicated to updates which checksum differs from already ran ones.
 * 
 * @author Guillaume Mary
 */
public class NonCompliantUpdateException extends ExecutionException {
	
	private final Map<ChangeSet, Checksum> nonCompliantUpdates;
	
	public NonCompliantUpdateException(String message, Map<ChangeSet, Checksum> nonCompliantUpdates) {
		super(message);
		this.nonCompliantUpdates = nonCompliantUpdates;
	}
	
	public Map<ChangeSet, Checksum> getNonCompliantUpdates() {
		return nonCompliantUpdates;
	}
}
