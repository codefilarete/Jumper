package org.gama.jumper;

import java.util.Objects;

/**
 * An identifier of an {@link Update}.
 * For instance a date, a ticket number, ... or a combination of them.
 * 
 * @author Guillaume Mary
 */
public class UpdateId {
	
	private final String identifier;
	
	public UpdateId(String identifier) {
		this.identifier = identifier;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		UpdateId updateId = (UpdateId) o;
		return Objects.equals(identifier, updateId.identifier);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(identifier);
	}
}
