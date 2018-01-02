package org.gama.jumper.impl;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Guillaume Mary
 */
public class ClassSignature {
	
	public byte[] giveSignature(Class c) {
		try (InputStream resourceAsStream = c.getResourceAsStream(c.getSimpleName().replace('.', '/') + ".class")) {
			return checksum(resourceAsStream, getMessageDigest());
		} catch (IOException | NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	public byte[] checksum(InputStream in, MessageDigest digest) throws IOException {
		byte[] block = new byte[4096];
		int length;
		while ((length = in.read(block)) > 0) {
			digest.update(block, 0, length);
		}
		return digest.digest();
	}
	
	public MessageDigest getMessageDigest() throws NoSuchAlgorithmException {
		return MessageDigest.getInstance("MD5");
	}
}
