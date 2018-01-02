package org.gama.jumper;

import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface ApplicationUpdatesStorage {
	
	void persist(Update update);
	
	Set<UpdateId> giveRanIdentifiers();
	
	<C extends Update> C get(UpdateId updateId, Class<C> aClass);
}
