package org.codefilarete.jumper;

/**
 * Defines interface for {@link Change}s that computes by themselves their checksum.
 * {@link org.codefilarete.jumper.impl.SupportedChange} checksum are computed by {@link ChangeSetRunner}.
 *
 * @author Guillaume Mary
 */
public interface ChecksumCapableChange extends Change {
	
	Checksum computeChecksum();
	
}
