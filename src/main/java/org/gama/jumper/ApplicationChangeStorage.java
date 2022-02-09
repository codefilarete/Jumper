package org.codefilarete.jumper;

import java.util.Map;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface ApplicationChangeStorage {
	
	void persist(Change change);
	
	Set<ChangeId> giveRanIdentifiers();
	
	Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> updates);
}
