package org.codefilarete.jumper;

/**
 * An identifier of a {@link ChangeSet}.
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
		return identifier.equals(changeId.identifier);
	}
	
	@Override
	public int hashCode() {
		return identifier.hashCode();
	}
	
	@Override
	public String toString() {
		return identifier;
	}
}
