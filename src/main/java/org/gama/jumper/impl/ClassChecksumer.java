package org.gama.jumper.impl;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.gama.jumper.Checksum;
import org.gama.jumper.Checksumer;

/**
 * A class aimed at creating a checksum from a class bytecode.
 * 
 * @author Guillaume Mary
 */
public class ClassChecksumer implements Checksumer<Class> {
	
	public static final ClassChecksumer INSTANCE = new ClassChecksumer();
	
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
	public Checksum checksum(Class c) {
		return new Checksum(DatatypeConverter.printHexBinary(buildChecksum(c)));
	}
	
	public byte[] buildChecksum(Class c) {
		String classFilePath = c.getName().replace('.', '/') + ".class";
		try (InputStream resourceAsStream = c.getClassLoader().getResourceAsStream(classFilePath)) {
			return buildChecksum(resourceAsStream, getMessageDigest());
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
