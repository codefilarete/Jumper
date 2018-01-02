package org.gama.jumper;

import java.util.concurrent.ExecutionException;

/**
 * @author Guillaume Mary
 */
public interface Update {
	
	UpdateId getIdentifier();
	
	boolean shouldAlwaysRun();
	
	void run() throws ExecutionException;
}
