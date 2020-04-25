package org.gama.jumper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class ApplicationUpdateProcessor {
	
	private ApplicationChangeStorage applicationChangeStorage;
	
	private ExecutionListener executionListener = new NoopExecutionListener();
	
	public ApplicationUpdateProcessor() {
	}
	
	public void setExecutionListener(ExecutionListener executionListener) {
		this.executionListener = executionListener;
	}
	
	public void processUpdates(List<Change> changes, Context context, ApplicationChangeStorage applicationChangeStorage) throws 
			ExecutionException {
		this.applicationChangeStorage = applicationChangeStorage;
		
		assertNonCompliantUpdates(changes);
		
		List<Change> updatesToRun = filterUpdatesToRun(changes, context);
		ApplicationUpdatesRunner applicationUpdatesRunner = new ApplicationUpdatesRunner(applicationChangeStorage);
		applicationUpdatesRunner.setExecutionListener(executionListener);
		applicationUpdatesRunner.run(updatesToRun, context);
	}
	
	private void assertNonCompliantUpdates(List<Change> changes) {
		// NB: we store current update Checksum in a Map to avoid its computation twice
		Map<Change, Checksum> nonCompliantUpdates = new LinkedHashMap<>(changes.size());
		Map<ChangeId, Checksum> currentlyStoredChecksums = applicationChangeStorage.giveChecksum(Iterables.collectToList(changes, Change::getIdentifier));
		changes.forEach(u -> {
			Checksum currentlyStoredChecksum = currentlyStoredChecksums.get(u.getIdentifier());
			if (currentlyStoredChecksum != null) {
				Checksum currentChecksum = u.computeChecksum();
				if (!currentlyStoredChecksum.equals(currentChecksum)
						|| u.getCompatibleChecksums().contains(currentChecksum)) {
					nonCompliantUpdates.put(u, currentlyStoredChecksum);
				}
			}
		});
		if (!nonCompliantUpdates.isEmpty()) {
			throw new NonCompliantUpdateException("Some changes have changed since last run. Add a compatible signature or review conflict", 
					nonCompliantUpdates);
		}
	}
	
	private List<Change> filterUpdatesToRun(List<Change> changes, Context context) {
		Set<ChangeId> ranIdentifiers = applicationChangeStorage.giveRanIdentifiers();
		return changes.stream()
				.filter(u -> shouldRun(u, ranIdentifiers, context))
				.collect(Collectors.toList());
	}
	
	/**
	 * Decides whether or not an Change must be run
	 *
	 * @param u the {@link Change} to be checked
	 * @param ranIdentifiers the already ran identifiers
	 * @return true to plan it for running
	 */
	protected boolean shouldRun(Change u, Set<ChangeId> ranIdentifiers, Context context) {
		boolean isAuthorizedToRun = !ranIdentifiers.contains(u.getIdentifier()) || u.shouldAlwaysRun();
		return isAuthorizedToRun && u.shouldRun(context);
	}
}
