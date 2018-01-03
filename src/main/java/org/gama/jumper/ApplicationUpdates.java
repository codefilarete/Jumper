package org.gama.jumper;

import java.util.ArrayList;
import java.util.List;

/**
 * A container of updates (to be executed or not)
 * 
 * @author Guillaume Mary
 */
public class ApplicationUpdates {
	
	private final List<Update> updates;
	
	public ApplicationUpdates() {
		this(new ArrayList<>());
	}
	
	public ApplicationUpdates(List<Update> updates) {
		this.updates = updates;
	}
	
	public List<Update> getUpdates() {
		return updates;
	}
}
