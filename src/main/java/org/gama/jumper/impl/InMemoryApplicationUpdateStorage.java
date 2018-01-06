package org.gama.jumper.impl;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.jumper.ApplicationUpdateStorage;
import org.gama.jumper.Checksum;
import org.gama.jumper.Update;
import org.gama.jumper.UpdateId;

/**
 * @author Guillaume Mary
 */
public class InMemoryApplicationUpdateStorage implements ApplicationUpdateStorage {
	
	private final Map<UpdateId, StoredUpdate> storage = new HashMap<>();
	
	@Override
	public void persist(Update update) {
		storage.put(update.getIdentifier(), new StoredUpdate(update.computeChecksum(), OffsetDateTime.now()));
	}
	
	@Override
	public Set<UpdateId> giveRanIdentifiers() {
		return storage.keySet();
	}
	
	@Override
	public Map<UpdateId, Checksum> giveChecksum(Iterable<UpdateId> updates) {
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
