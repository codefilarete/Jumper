package org.gama.jumper.impl;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.gama.jumper.Checksum;
import org.gama.jumper.Checksumer;

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
	
	private static final MessageDigest MD5_ALGORITHM;
	
	static {
		try {
			MD5_ALGORITHM = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// Should not happen because MD5 is a JVM default algorithm
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Checksum checksum(String s) {
		try {
			return new Checksum(DatatypeConverter.printHexBinary(buildChecksum(new ByteArrayInputStream(s.getBytes(CHARSET_FOR_BYTES)), getMessageDigest())));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public byte[] buildChecksum(InputStream in, MessageDigest digest) throws IOException {
		byte[] block = new byte[4096];
		int length;
		while ((length = in.read(block)) > 0) {
			digest.update(block, 0, length);
		}
		return digest.digest();
	}
	
	public MessageDigest getMessageDigest() {
		return MD5_ALGORITHM;
	}
}
