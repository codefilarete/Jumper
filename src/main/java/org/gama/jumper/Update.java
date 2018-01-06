package org.gama.jumper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface Update {
	
	UpdateId getIdentifier();
	
	/**
	 * Indicates if this {@link Update} must be executed even if it was already ran. Default is no (false).
	 * May be overriden according to task.
	 */
	default boolean shouldAlwaysRun() {
		return false;
	}
	
	/**
	 * Indicates if this {@link Update} must be run on the given {@link Context}. Default is yes (true).
	 * Should be overriden to implement a conditionnal reason of execution according to {@link Context}. 
	 */
	default boolean shouldRun(Context context) {
		return true;
	}
	
	void run() throws ExecutionException;
	
	/**
	 * Computes the checksum of the execution. Checksum must be considered as a signature of the business logic of the update.
	 * 
	 * @return a "busness logic"-relied Checksum
	 */
	Checksum computeChecksum();
	
	/**
	 * Interface for {@link Update}s that are signed with a MD5, SHA1, or whatever.
	 * Aimed at being used to check if this {@link Update} has changed since previous execution. So storage must record signature.
	 *
	 * @author Guillaume Mary
	 */
	default Set<Checksum> getCompatibleChecksums() {
		return Collections.EMPTY_SET;
	}
}
