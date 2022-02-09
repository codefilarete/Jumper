package org.codefilarete.jumper;

/**
 * A class dedicated to {@link Change} execution.
 * 
 * @author Guillaume Mary
 */
public class ApplicationUpdatesRunner {
	
	private final ApplicationChangeStorage applicationChangeStorage;
	
	private ExecutionListener executionListener = new NoopExecutionListener();
	
	public ApplicationUpdatesRunner(ApplicationChangeStorage applicationChangeStorage) {
		this.applicationChangeStorage = applicationChangeStorage;
	}
	
	public void setExecutionListener(ExecutionListener executionListener) {
		this.executionListener = executionListener;
	}
	
	public void run(Iterable<Change> updatesToRun, Context context) throws ExecutionException {
		for (Change change : updatesToRun) {
			executionListener.beforeRun(change);
			run(change, context);
			executionListener.afterRun(change);
			persistState(change);
		}
	}
	
	private void run(Change change, Context context) throws ExecutionException {
		try {
			change.run(context);
		} catch (RuntimeException | OutOfMemoryError e) {
			throw new ExecutionException(e);
		}
	}
	
	private void persistState(Change change) throws ExecutionException {
		try {
			applicationChangeStorage.persist(change);
		} catch (RuntimeException | OutOfMemoryError e) {
			throw new ExecutionException("State of change " + change.getIdentifier() + " couldn't be stored", e);
		}
	}
}
