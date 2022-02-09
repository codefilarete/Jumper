package org.codefilarete.jumper;

import java.util.Objects;

/**
 * An identifier of an {@link Change}.
 * For instance a date, a ticket number, ... or a combination of them.
 * 
 * @author Guillaume Mary
 */
public class ChangeId {
	
	private final String identifier;
	
	public ChangeId(String identifier) {
		this.identifier = identifier;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ChangeId changeId = (ChangeId) o;
		return Objects.equals(identifier, changeId.identifier);
	}
	
	@Override
	public int hashCode() {
		// implementation is based on same fields as equals() to satisfy hashCode() contract
		return Objects.hash(identifier);
	}
	
	@Override
	public String toString() {
		return identifier;
	}
}
