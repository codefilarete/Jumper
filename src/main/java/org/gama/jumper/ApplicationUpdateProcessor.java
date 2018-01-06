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
	
	private ApplicationUpdateStorage applicationUpdateStorage;
	
	private ExecutionListener executionListener = new NoopExecutionListener();
	
	public ApplicationUpdateProcessor() {
	}
	
	public void setExecutionListener(ExecutionListener executionListener) {
		this.executionListener = executionListener;
	}
	
	public void processUpdates(List<Update> updates, Context context, ApplicationUpdateStorage applicationUpdateStorage) throws 
			ExecutionException {
		this.applicationUpdateStorage = applicationUpdateStorage;
		
		assertNonCompliantUpdates(updates);
		
		List<Update> updatesToRun = filterUpdatesToRun(updates, context);
		ApplicationUpdatesRunner applicationUpdatesRunner = new ApplicationUpdatesRunner(applicationUpdateStorage);
		applicationUpdatesRunner.setExecutionListener(executionListener);
		applicationUpdatesRunner.run(updatesToRun);
	}
	
	private void assertNonCompliantUpdates(List<Update> updates) {
		// NB: we store current update Checksum in a Map to avoid its computation twice
		Map<Update, Checksum> nonCompliantUpdates = new LinkedHashMap<>(updates.size());
		Map<UpdateId, Checksum> currentlyStoredChecksums = applicationUpdateStorage.giveChecksum(Iterables.collectToList(updates, Update::getIdentifier));
		updates.forEach(u -> {
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
			throw new NonCompliantUpdateException("Some updates have changed since last run. Add a compatible signature or review conflict", 
					nonCompliantUpdates);
		}
	}
	
	private List<Update> filterUpdatesToRun(List<Update> updates, Context context) {
		Set<UpdateId> ranIdentifiers = applicationUpdateStorage.giveRanIdentifiers();
		return updates.stream()
				.filter(u -> shouldRun(u, ranIdentifiers, context))
				.collect(Collectors.toList());
	}
	
	/**
	 * Decides whether or not an Update must be run
	 *
	 * @param u the {@link Update} to be checked
	 * @param ranIdentifiers the already ran identifiers
	 * @return true to plan it for running
	 */
	protected boolean shouldRun(Update u, Set<UpdateId> ranIdentifiers, Context context) {
		boolean isAuthorizedToRun = !ranIdentifiers.contains(u.getIdentifier()) || u.shouldAlwaysRun();
		return isAuthorizedToRun && u.shouldRun(context);
	}
}
