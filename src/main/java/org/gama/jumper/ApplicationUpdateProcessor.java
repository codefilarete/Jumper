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
		ApplicationUpdatesRunner applicationUpdatesRunner = new ApplicationUpdatesRunner(applicationUpdatesStorage);
		applicationUpdatesRunner.setExecutionListener(executionListener);
		applicationUpdatesRunner.run(giveUpdatesToRun());
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
