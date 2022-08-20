package org.codefilarete.jumper;

import java.util.Map;
import java.util.Set;

/**
 * Contract for persisting {@link Change} signatures, then, making it available for future checking of runnable {@link Change}s.
 *
 * @author Guillaume Mary
 */
public interface ChangeStorage {
	
	void persist(ChangeSignet change);
	
	/**
	 * Gives all {@link Change} identifiers stored in this storage
	 *
	 * @return all {@link Change} identifiers stored in this storage
	 */
	Set<ChangeId> giveRanIdentifiers();
	
	/**
	 * Gives checksums stored in this storage for given {@link ChangeId}
	 *
	 * @param changes {@link ChangeId}s for which {@link Checksum} must be returned
	 * @return checksums stored in this storage for given {@link ChangeId}
	 */
	Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> changes);
	
	/**
	 * A {@link Change} signature
	 *
	 * @author Guillaume Mary
	 */
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
