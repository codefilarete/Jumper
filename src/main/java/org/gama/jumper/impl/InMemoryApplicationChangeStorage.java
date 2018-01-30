package org.gama.jumper.impl;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.jumper.ApplicationChangeStorage;
import org.gama.jumper.Checksum;
import org.gama.jumper.Change;
import org.gama.jumper.ChangeId;

/**
 * @author Guillaume Mary
 */
public class InMemoryApplicationChangeStorage implements ApplicationChangeStorage {
	
	private final Map<ChangeId, StoredUpdate> storage = new HashMap<>();
	
	@Override
	public void persist(Change change) {
		storage.put(change.getIdentifier(), new StoredUpdate(change.computeChecksum(), OffsetDateTime.now()));
	}
	
	@Override
	public Set<ChangeId> giveRanIdentifiers() {
		return storage.keySet();
	}
	
	@Override
	public Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> updates) {
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
