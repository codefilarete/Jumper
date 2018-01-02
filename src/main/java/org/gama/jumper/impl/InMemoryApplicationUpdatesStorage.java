package org.gama.jumper.impl;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.gama.jumper.ApplicationUpdatesStorage;
import org.gama.jumper.Update;
import org.gama.jumper.UpdateId;

/**
 * @author Guillaume Mary
 */
public class InMemoryApplicationUpdatesStorage implements ApplicationUpdatesStorage {
	
	private final Map<UpdateId, StoredUpdate> storage = new HashMap<>();
	
	@Override
	public void persist(Update update) {
		storage.put(update.getIdentifier(), new StoredUpdate(update, OffsetDateTime.now()));
	}
	
	@Override
	public Set<UpdateId> giveRanIdentifiers() {
		return storage.keySet();
	}
	
	@Override
	public <C extends Update> C get(UpdateId updateId, Class<C> aClass) {
		return (C) storage.getOrDefault(updateId, null);
	}
	
	private static class StoredUpdate {
		
		private final Update update;
		private final OffsetDateTime storageTime;
		
		private StoredUpdate(Update update, OffsetDateTime storageTime) {
			this.update = update;
			this.storageTime = storageTime;
		}
	}
}
