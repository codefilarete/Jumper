package org.codefilarete.jumper;

public interface ChecksumCapableChange extends Change {
	
	Checksum computeChecksum();
	
}
