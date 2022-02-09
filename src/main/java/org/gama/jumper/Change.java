package org.codefilarete.jumper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface Change {
	
	ChangeId getIdentifier();
	
	/**
	 * Indicates if this {@link Change} must be executed even if it was already ran. Default is no (false).
	 * May be overriden according to task.
	 */
	default boolean shouldAlwaysRun() {
		return false;
	}
	
	/**
	 * Indicates if this {@link Change} must be run on the given {@link Context}. Default is yes (true).
	 * Should be overriden to implement a conditionnal reason of execution according to {@link Context}. 
	 */
	default boolean shouldRun(Context context) {
		return true;
	}
	
	void run(Context context) throws ExecutionException;
	
	/**
	 * Computes the checksum of the execution. Checksum must be considered as a signature of the business logic of the update.
	 * 
	 * @return a "busness logic"-relied Checksum
	 */
	Checksum computeChecksum();
	
	/**
	 * Interface for {@link Change}s that are signed with a MD5, SHA1, or whatever.
	 * Aimed at being used to check if this {@link Change} has changed since previous execution. So storage must record signature.
	 *
	 * @author Guillaume Mary
	 */
	default Set<Checksum> getCompatibleChecksums() {
		return Collections.EMPTY_SET;
	}
}
