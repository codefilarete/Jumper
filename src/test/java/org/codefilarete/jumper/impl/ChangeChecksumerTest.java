package org.codefilarete.jumper.impl;

import org.assertj.core.api.Assertions;
import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;
import org.codefilarete.tool.exception.NotImplementedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChangeChecksumerTest {
	
	@Test
	void giveSignature() {
		ChangeChecksumer testInstance = new ChangeChecksumer();
		Assertions.assertThatCode(() -> {
			testInstance.giveSignature(new DDLStatement() {
			});
		}).isInstanceOf(NotImplementedException.class)
				.hasMessageStartingWith("Checksum computation is not implemented for ");
	}
}