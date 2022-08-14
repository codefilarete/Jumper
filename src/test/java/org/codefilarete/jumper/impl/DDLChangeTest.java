package org.codefilarete.jumper.impl;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DDLChangeTest {
	
	@Test
	void ddlChange_shouldRunOnce() {
		DDLChange testInstance = new DDLChange("x", new NewTable("y"));
		assertThat(testInstance.shouldAlwaysRun()).isFalse();
	}
	
}