package org.gama.jumper;

import java.util.Objects;

/**
 * A container for a MD5, SHA1, version, ... whatever that can represent a "signature" of an {@link Change}.
 * 
 * This will help to warn user about conflicting signature between two updates of same id : on one hand the already executed one, on the other hand
 * the one planed to be executed. Conflicts can result of:
 * - reused {@link UpdateId} between two {@link Change}s
 * - code changes made by developpers on {@link Change}s that were already run on target environment
 * 
 * @author Guillaume Mary
 */
public class Checksum {
	
	private final String checksum;
	
	public Checksum(String checksum) {
		this.checksum = checksum;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Checksum updateId = (Checksum) o;
		return Objects.equals(checksum, updateId.checksum);
	}
	
	@Override
	public int hashCode() {
		return checksum.hashCode();
	}
	
	@Override
	public String toString() {
		return checksum;
	}
}
