package org.gama.jumper;

import java.util.Map;

/**
 * Exception dedicated to updates whose checksum differs from already ran ones.
 * 
 * @author Guillaume Mary
 */
public class NonCompliantUpdateException extends ExecutionException {
	
	private final Map<Change, Checksum> nonCompliantUpdates;
	
	public NonCompliantUpdateException(String message, Map<Change, Checksum> nonCompliantUpdates) {
		super(message);
		this.nonCompliantUpdates = nonCompliantUpdates;
	}
	
	public Map<Change, Checksum> getNonCompliantUpdates() {
		return nonCompliantUpdates;
	}
}
