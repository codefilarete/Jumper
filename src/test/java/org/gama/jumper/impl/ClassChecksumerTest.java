package org.gama.jumper.impl;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class ClassChecksumerTest {
	
	@Test
	public void testChecksum() {
		ClassChecksumer testInstance = new ClassChecksumer();
		
		// asking several time the checksum of the same class gives the same result
		byte[] checksum1 = testInstance.buildChecksum(ChecksumTargetClass.class);
		byte[] checksum2 = testInstance.buildChecksum(ChecksumTargetClass.class);
		assertNotNull(checksum1);
		assertTrue(checksum1.length > 0);
		assertArrayEquals(checksum1, checksum2);
		// asking it with antother test instance gives same result
		byte[] checksum3 = new ClassChecksumer().buildChecksum(ChecksumTargetClass.class);
		assertArrayEquals(checksum1, checksum3);
	}
	
	public static class ChecksumTargetClass {
		
	}
	
}