package org.codefilarete.jumper;

import java.util.Objects;

/**
 * A container for a MD5, SHA1, version, ... whatever that can represent a "signature" of a {@link ChangeSet}.
 * 
 * This will help to warn user about conflicting signature between two updates of same id : on one hand the already executed one, on the other hand
 * the one planed to be executed. Conflicts can result of:
 * - reused {@link ChangeId} between two {@link ChangeSet}s
 * - code changes made by developers on {@link ChangeSet}s that were already run on target environment
 * 
 * @author Guillaume Mary
 */
public class Checksum {
	
	private final String checksum;
	
	/**
	 * Default single constructor with mandatory argument.
	 * 
	 * @param checksum expected to be a footprint af a {@link ChangeSet}, see {@link org.codefilarete.jumper.impl.StringChecksumer#checksum} for a basic implementation
	 */
	public Checksum(String checksum) {
		this.checksum = checksum;
	}
	
	/**
	 * Implemented to satisfy {@link java.util.Set#contains(Object)} while looking for compatible {@link Checksum} of a {@link ChangeSet}
	 * @param o any other {@link Object}
	 * @return true if this instance as same checksum than given one
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Checksum updateId = (Checksum) o;
		return Objects.equals(checksum, updateId.checksum);
	}
	
	/**
	 * Implemented according to {@link #equals(Object)} implementation
	 * @return the checksum value hashcode
	 */
	@Override
	public int hashCode() {
		return checksum.hashCode();
	}
	
	@Override
	public String toString() {
		return checksum;
	}
}
