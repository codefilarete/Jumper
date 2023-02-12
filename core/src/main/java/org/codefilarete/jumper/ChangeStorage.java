package org.codefilarete.jumper;

import java.util.Map;
import java.util.Set;

/**
 * Contract for persisting {@link ChangeSet} signatures, then, making it available for future checking of runnable {@link ChangeSet}s.
 *
 * @author Guillaume Mary
 */
public interface ChangeStorage {
	
	void persist(ChangeSignet change);
	
	/**
	 * Gives all {@link ChangeSet} identifiers stored in this storage
	 *
	 * @return all {@link ChangeSet} identifiers stored in this storage
	 */
	Set<ChangeSetId> giveRanIdentifiers();
	
	/**
	 * Gives checksums stored in this storage for given {@link ChangeSetId}
	 *
	 * @param changes {@link ChangeSetId}s for which {@link Checksum} must be returned
	 * @return checksums stored in this storage for given {@link ChangeSetId}
	 */
	Map<ChangeSetId, Checksum> giveChecksum(Iterable<ChangeSetId> changes);
	
	/**
	 * A {@link ChangeSet} signature
	 *
	 * @author Guillaume Mary
	 */
	class ChangeSignet {
		
		private final ChangeSetId changeSetId;
		private final Checksum checksum;
		
		public ChangeSignet(String identifier, Checksum checksum) {
			this(new ChangeSetId(identifier), checksum);
		}
		
		public ChangeSignet(ChangeSetId changeSetId, Checksum checksum) {
			this.changeSetId = changeSetId;
			this.checksum = checksum;
		}
		
		public ChangeSetId getChangeId() {
			return changeSetId;
		}
		
		public Checksum getChecksum() {
			return checksum;
		}
	}
}
