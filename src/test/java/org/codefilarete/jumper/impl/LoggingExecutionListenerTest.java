package org.codefilarete.jumper.impl;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.jumper.ChangeSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.CHAR_SEQUENCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

class LoggingExecutionListenerTest {
	
	@Test
	void loggerIsFilled() throws InterruptedException {
		Logger logger = Mockito.mock(Logger.class);
		List<String> logs = new ArrayList<>();
		doAnswer(invocation -> {
			logs.add("DEBUG: " + invocation.getArgument(0).toString().replace("{}", invocation.getArgument(1).toString()));
			return null;
		}).when(logger).debug(anyString(), (Object) any());
		doAnswer(invocation -> {
			logs.add("TRACE: " + invocation.getArgument(0).toString().replace("{}", invocation.getArgument(1).toString()));
			return null;
		}).when(logger).trace(anyString(), (Object) any());
		
		
		LoggingExecutionListener testInstance = new LoggingExecutionListener(logger);
		testInstance.beforeProcess();
		ChangeSet change = new ChangeSet("x", false);
		testInstance.beforeRun(change);
		Thread.sleep(50);
		testInstance.afterRun(change);
		testInstance.afterRun("toto", 42L);
		Thread.sleep(50);
		testInstance.afterProcess();
		
		assertThat(logs).element(0, CHAR_SEQUENCE).isEqualTo("DEBUG: Executing x");
		assertThat(logs).element(1, CHAR_SEQUENCE).matches("DEBUG: Execution took 0m 0s \\d+ms");
		assertThat(logs).element(2, CHAR_SEQUENCE).isEqualTo("TRACE: Updated row count 42");
		assertThat(logs).element(3, CHAR_SEQUENCE).matches("DEBUG: Total execution took 0m 0s \\d+ms");
	}
	
}