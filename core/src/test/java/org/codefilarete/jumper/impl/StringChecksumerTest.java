package org.codefilarete.jumper.impl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringChecksumerTest {
	
	@Test
	void checksum() {
		StringChecksumer testInstance = new StringChecksumer();
		assertThat(testInstance.checksum("Hello world !").toString()).isEqualTo("67C18D060479C5D867C9B91C80EDEB4C");
		// a second execution should give same result
		assertThat(testInstance.checksum("Hello world !").toString()).isEqualTo("67C18D060479C5D867C9B91C80EDEB4C");
	}
	
}