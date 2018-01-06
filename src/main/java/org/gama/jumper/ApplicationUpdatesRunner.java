package org.gama.jumper;

/**
 * A class dedicated to {@link Update} execution.
 * 
 * @author Guillaume Mary
 */
public class ApplicationUpdatesRunner {
	
	private final ApplicationUpdateStorage applicationUpdateStorage;
	
	private ExecutionListener executionListener = new NoopExecutionListener();
	
	public ApplicationUpdatesRunner(ApplicationUpdateStorage applicationUpdateStorage) {
		this.applicationUpdateStorage = applicationUpdateStorage;
	}
	
	public void setExecutionListener(ExecutionListener executionListener) {
		this.executionListener = executionListener;
	}
	
	public void run(Iterable<Update> updatesToRun) throws ExecutionException {
		for (Update update : updatesToRun) {
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
			applicationUpdateStorage.persist(update);
		} catch (RuntimeException | OutOfMemoryError e) {
			throw new ExecutionException("State of update " + update.getIdentifier() + " couldn't be stored", e);
		}
	}
}
