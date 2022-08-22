package org.codefilarete.jumper;

/**
 * A simple contract to listen to an execution update.
 * 
 * @author Guillaume Mary
 */
public interface ChangeSetExecutionListener {
	
	/**
	 * Is called before the execution of the given change
	 * @param change the change that's going to be executed
	 */
	void beforeRun(Change change);
	
	/**
	 * Is called after the execution of the given change
	 * @param change the executed change
	 */
	void afterRun(Change change);
	
	/**
	 * Is called at the very beginning of update process
	 */
	void beforeProcess();
	
	/**
	 * Is called at the end of update process
	 */
	void afterProcess();
	
	/**
	 * To be implemented by listeners that need to be notified of statement treatment
	 *
	 * @author Guillaume Mary
	 */
	interface FineGrainExecutionListener extends ChangeSetExecutionListener {
		
		/**
		 * Is called after each sql statement run by the processor
		 *
		 * @param statement sql statement run
		 * @param updatedRowCount updated row count updated by sql statement (optional), see {@link java.sql.Statement#executeLargeUpdate(String)}
		 */
		void afterRun(String statement, Long updatedRowCount);
		
	}
}
