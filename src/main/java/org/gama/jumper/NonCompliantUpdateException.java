package org.gama.jumper;

import java.util.Map;

/**
 * Exception dedicated to updates whose checksum differs from already ran ones.
 * 
 * @author Guillaume Mary
 */
public class NonCompliantUpdateException extends ExecutionException {
	
	private final Map<Update, Checksum> nonCompliantUpdates;
	
	public NonCompliantUpdateException(String message, Map<Update, Checksum> nonCompliantUpdates) {
		super(message);
		this.nonCompliantUpdates = nonCompliantUpdates;
	}
	
	public Map<Update, Checksum> getNonCompliantUpdates() {
		return nonCompliantUpdates;
	}
}
