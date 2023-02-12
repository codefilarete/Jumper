package org.codefilarete.jumper;

/**
 * An identifier of a {@link ChangeSet}.
 * For instance a date, a ticket number, ... or a combination of them.
 * 
 * @author Guillaume Mary
 */
public class ChangeSetId {
	
	private final String identifier;
	
	public ChangeSetId(String identifier) {
		this.identifier = identifier;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ChangeSetId changeSetId = (ChangeSetId) o;
		return identifier.equals(changeSetId.identifier);
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
