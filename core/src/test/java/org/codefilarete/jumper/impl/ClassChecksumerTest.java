package org.codefilarete.jumper.impl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ClassChecksumerTest {
	
	@Test
	public void checksum_steadiness() {
		ClassChecksumer testInstance = new ClassChecksumer();
		
		// asking several time the checksum of the same class gives the same result
		byte[] checksum1 = testInstance.buildChecksum(ChecksumTargetClass.class);
		byte[] checksum2 = testInstance.buildChecksum(ChecksumTargetClass.class);
		assertThat(checksum1).isNotNull();
		assertThat(checksum1.length > 0).isTrue();
		assertThat(checksum2).isEqualTo(checksum1);
		// asking it with another test instance gives same result
		byte[] checksum3 = new ClassChecksumer().buildChecksum(ChecksumTargetClass.class);
		assertThat(checksum3).isEqualTo(checksum1);
	}
	
	public static class ChecksumTargetClass {
		
	}
	
}