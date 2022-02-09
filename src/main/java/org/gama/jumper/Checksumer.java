package org.codefilarete.jumper;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface Checksumer<T> {
	
	Checksum checksum(T source);
}
