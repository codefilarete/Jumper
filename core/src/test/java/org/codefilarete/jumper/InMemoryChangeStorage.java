package org.codefilarete.jumper;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Guillaume Mary
 */
public class InMemoryChangeStorage implements ChangeStorage {
	
	private final Map<ChangeSetId, StoredUpdate> storage = new HashMap<>();
	
	@Override
	public void persist(ChangeSignet change) {
		storage.put(change.getChangeId(), new StoredUpdate(change.getChecksum(), OffsetDateTime.now()));
	}
	
	@Override
	public Set<ChangeSetId> giveRanIdentifiers() {
		return storage.keySet();
	}
	
	@Override
	public Map<ChangeSetId, Checksum> giveChecksum(Iterable<ChangeSetId> changes) {
		return storage.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getChecksum()));
	}
	
	private static class StoredUpdate {
		
		private final Checksum checksum;
		private final OffsetDateTime storageTime;
		
		private StoredUpdate(Checksum checksum, OffsetDateTime storageTime) {
			this.checksum = checksum;
			this.storageTime = storageTime;
		}
		
		private Checksum getChecksum() {
			return checksum;
		}
		
		private OffsetDateTime getStorageTime() {
			return storageTime;
		}
	}
}
