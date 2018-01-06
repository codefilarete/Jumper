package org.gama.jumper;

import java.util.Map;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface ApplicationUpdateStorage {
	
	void persist(Update update);
	
	Set<UpdateId> giveRanIdentifiers();
	
	Map<UpdateId, Checksum> giveChecksum(Iterable<UpdateId> updates);
}
