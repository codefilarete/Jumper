package org.codefilarete.jumper;

/**
 * A simple interface that defines the contract to acquire and release the lock of a database update process.
 *
 * @author Guillaume Mary
 */
public interface UpdateProcessSemaphore {
	
	void acquireLock(String identifier);
	
	void releaseLock(String identifier);
}
