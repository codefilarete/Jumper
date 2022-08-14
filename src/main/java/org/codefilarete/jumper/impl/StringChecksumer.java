package org.codefilarete.jumper.impl;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.Checksumer;

import static org.codefilarete.jumper.impl.ByteChecksumer.toHexBinaryString;

/**
 * A class aimed at creating a checksum from a String, based on a MD5 algorithm.
 * Thought for basic, general usage.
 * 
 * @author Guillaume Mary
 */
public class StringChecksumer implements Checksumer<String> {
	
	/**
	 * Singleton to be used for simple case
	 */
	public static final StringChecksumer INSTANCE = new StringChecksumer();
	
	/** Charset of String bytes. Fixed to prevent from being dependent of JVM default's */
	private static final Charset CHARSET_FOR_BYTES = StandardCharsets.UTF_8;
	
	private final ByteChecksumer byteChecksumer = new ByteChecksumer();
	
	@Override
	public Checksum checksum(String s) {
		return new Checksum(toHexBinaryString(byteChecksumer.buildChecksum(s.getBytes(CHARSET_FOR_BYTES))));
	}
}
