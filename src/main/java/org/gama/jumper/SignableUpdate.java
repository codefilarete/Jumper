package org.gama.jumper;

/**
 * Interface for {@link Update}s that are signed with a CheckSum, SHA1, or whatever.
 * Aimed at being used to check if this {@link Update} has changed since previous execution. So storage must record signature.
 * 
 * @author Guillaume Mary
 */
public interface SignableUpdate extends Update {
	
	String getSignature();
}
