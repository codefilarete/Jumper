package org.gama.jumper;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Guillaume Mary
 */
public class ApplicationUpdateProcessor {
	
	private final ApplicationUpdates applicationUpdates;
	
	private final ApplicationUpdatesStorage applicationUpdatesStorage;
	
	public ApplicationUpdateProcessor(ApplicationUpdates applicationUpdates, ApplicationUpdatesStorage applicationUpdatesStorage) {
		this.applicationUpdates = applicationUpdates;
		this.applicationUpdatesStorage = applicationUpdatesStorage;
	}
	
	public void runUpdates() {
		for (Update update : giveUpdatesToRun()) {
			run(update);
		}
	}
	
	private void run(Update update) {
		try {
			update.run();
		} catch (RuntimeException e) {
			
		} catch (java.util.concurrent.ExecutionException e) {
			e.printStackTrace();
		}
		try {
			applicationUpdatesStorage.persist(update);
		} catch (RuntimeException e) {
			
		}
	}
	
	private List<Update> giveUpdatesToRun() {
		Set<UpdateId> ranIdentifiers = applicationUpdatesStorage.giveRanIdentifiers();
		return applicationUpdates.getUpdates().stream()
				.filter(u -> shouldRun(u, ranIdentifiers))
				.collect(Collectors.toList());
	}
	
	private boolean shouldRun(Update u, Set<UpdateId> ranIdentifiers) {
		return !ranIdentifiers.contains(u.getIdentifier())
				|| u.shouldAlwaysRun()
				|| (u instanceof VersatileUpdate && ((VersatileUpdate) u).hasChanged())
		;
	}
}
