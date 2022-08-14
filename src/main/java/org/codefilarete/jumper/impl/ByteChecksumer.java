package org.codefilarete.jumper.impl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Helper that creates a checksum from some bytes
 *
 * @see #buildChecksum(byte[])
 * @author Guillaume Mary
 */
class ByteChecksumer {
	
	private static final char[] hexCode = new char[] {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'A', 'B', 'C', 'D', 'E', 'F' };
	
	static String toHexBinaryString(byte[] data) {
		StringBuilder r = new StringBuilder(data.length * 2);
		for (byte b : data) {
			r.append(hexCode[(b >> 4) & 0xF]).append(hexCode[(b & 0xF)]);
		}
		return r.toString();
	}
	
	private static final MessageDigest MD5_ALGORITHM;
	
	static {
		try {
			MD5_ALGORITHM = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// Should not happen because MD5 is a JVM default algorithm
			throw new RuntimeException(e);
		}
	}
	
	private final MessageDigest messageDigest;
	
	public ByteChecksumer() {
		this(MD5_ALGORITHM);
	}
	
	public ByteChecksumer(MessageDigest messageDigest) {
		this.messageDigest = messageDigest;
	}
	
	byte[] buildChecksum(byte[] in) {
		final int blockSize = 4096;
		int i = 0;
		for (; i < in.length / blockSize; i++) {
			messageDigest.update(in, i * blockSize, blockSize);
		}
		int lastBlockLen = in.length % blockSize;
		if (lastBlockLen > 0) {
			messageDigest.update(in, i * blockSize, lastBlockLen);
		}
		return messageDigest.digest();
	}
	
}
