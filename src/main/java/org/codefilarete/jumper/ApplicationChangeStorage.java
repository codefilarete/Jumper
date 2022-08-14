package org.codefilarete.jumper;

import java.util.Map;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface ApplicationChangeStorage {
	
	void persist(ChangeSignet change);
	
	Set<ChangeId> giveRanIdentifiers();
	
	Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> updates);
	
	class ChangeSignet {
		private final ChangeId changeId;
		private final Checksum checksum;
		
		public ChangeSignet(ChangeId changeId, Checksum checksum) {
			this.changeId = changeId;
			this.checksum = checksum;
		}
		
		public ChangeId getChangeId() {
			return changeId;
		}
		
		public Checksum getChecksum() {
			return checksum;
		}
	}
}
