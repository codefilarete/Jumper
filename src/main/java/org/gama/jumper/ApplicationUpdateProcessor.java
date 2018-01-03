package org.gama.jumper;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author Guillaume Mary
 */
public class ApplicationUpdateProcessor {
	
	private final ApplicationUpdates applicationUpdates;
	
	private final ApplicationUpdatesStorage applicationUpdatesStorage;
	
	private ExecutionListener executionListener = new NoopExecutionListener();
	
	public ApplicationUpdateProcessor(ApplicationUpdates applicationUpdates, ApplicationUpdatesStorage applicationUpdatesStorage) {
		this.applicationUpdates = applicationUpdates;
		this.applicationUpdatesStorage = applicationUpdatesStorage;
	}
	
	public void setExecutionListener(ExecutionListener executionListener) {
		this.executionListener = executionListener;
	}
	
	public void runUpdates() throws ExecutionException {
		for (Update update : giveUpdatesToRun()) {
			executionListener.beforeRun(update);
			run(update);
			executionListener.afterRun(update);
			persistState(update);
		}
	}
	
	private void run(Update update) throws ExecutionException {
		try {
			update.run();
		} catch (RuntimeException | OutOfMemoryError e) {
			throw new ExecutionException(e);
		}
	}
	
	private void persistState(Update update) throws ExecutionException {
		try {
			applicationUpdatesStorage.persist(update);
		} catch (RuntimeException | OutOfMemoryError e) {
			throw new ExecutionException("State of update " + update.getIdentifier() + " couldn't be stored", e);
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
