package org.codefilarete.jumper.impl;

import java.io.IOException;
import java.io.InputStream;

import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.Checksumer;
import org.codefilarete.tool.io.IOs;

/**
 * A class aimed at creating a checksum from a class bytecode.
 * 
 * @author Guillaume Mary
 */
public class ClassChecksumer implements Checksumer<Class> {
	
	public static final ClassChecksumer INSTANCE = new ClassChecksumer();
	
	private final ByteChecksumer byteChecksumer = new ByteChecksumer();
	
	@Override
	public Checksum checksum(Class c) {
		return new Checksum(ByteChecksumer.toHexBinaryString(buildChecksum(c)));
	}
	
	public byte[] buildChecksum(Class c) {
		String classFilePath = c.getName().replace('.', '/') + ".class";
		try (InputStream resourceAsStream = c.getClassLoader().getResourceAsStream(classFilePath)) {
			return byteChecksumer.buildChecksum(IOs.toByteArray(resourceAsStream, (int) IOs._512_Ko));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
