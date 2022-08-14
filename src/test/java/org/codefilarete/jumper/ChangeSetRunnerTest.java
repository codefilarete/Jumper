package org.codefilarete.jumper;

import java.sql.Connection;
import java.util.List;

import org.codefilarete.jumper.ddl.engine.Dialect;
import org.codefilarete.jumper.DialectResolver.DatabaseSignet;
import org.codefilarete.jumper.impl.AbstractJavaChange;
import org.codefilarete.jumper.impl.InMemoryApplicationChangeStorage;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Guillaume Mary
 */
public class ChangeSetRunnerTest {
	
	private static class LocalChangeRunner extends ChangeSetRunner {
		
		private final Context context = new Context(new DatabaseSignet("", 1, 0));
		
		private final InMemoryApplicationChangeStorage applicationChangeStorage;
		
		private LocalChangeRunner(List<Change> changes) {
			this(changes, new InMemoryApplicationChangeStorage());
		}
		
		private LocalChangeRunner(List<Change> changes, InMemoryApplicationChangeStorage applicationChangeStorage) {
			super(changes, () -> null, applicationChangeStorage);
			this.applicationChangeStorage = applicationChangeStorage;
		}
		
		@Override
		protected Context buildContext(Connection connection) {
			return context;
		}
		
		@Override
		protected Dialect giveDialect(DatabaseSignet databaseSignet) {
			return new Dialect();
		}
	}
	
	@Test
	void processUpdates_runNonRanUpdates() {
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		AbstractJavaChange dummyUpdate = new AbstractJavaChange("dummyId", false) {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		LocalChangeRunner testInstance = new LocalChangeRunner(Arrays.asList(dummyUpdate));
		testInstance.processUpdates();
		
		// Change must be ran
		assertThat(executionCounter.getValue()).isEqualTo(1);
		// id must be stored
		assertThat(testInstance.applicationChangeStorage.giveRanIdentifiers()).isEqualTo(Arrays.asSet(new ChangeId("dummyId")));
		
		// second execution
		testInstance.processUpdates();
		
		// Change mustn't be run again because it is marked as not always run
		assertThat(executionCounter.getValue()).isEqualTo(1);
	}
	
	@Test
	void processUpdates_alwaysRun() {
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		AbstractChange dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		LocalChangeRunner testInstance = new LocalChangeRunner(Arrays.asList(dummyUpdate));
		testInstance.processUpdates();
		
		// Change must be ran
		assertThat(executionCounter.getValue()).isEqualTo(1);
		// id must be stored
		assertThat(testInstance.applicationChangeStorage.giveRanIdentifiers()).isEqualTo(Arrays.asSet(new ChangeId("dummyId")));
		
		// second execution
		testInstance.processUpdates();
		
		// Change must be run again because it is marked as always run
		assertThat(executionCounter.getValue()).isEqualTo(2);
	}
	
	@Test
	void processUpdates_checksumMismatch() {
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		AbstractChange dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		LocalChangeRunner testInstance = new LocalChangeRunner(Arrays.asList(dummyUpdate));
		testInstance.processUpdates();
		
		// Change must be ran
		assertThat(executionCounter.getValue()).isEqualTo(1);
		// id must be stored
		assertThat(testInstance.applicationChangeStorage.giveRanIdentifiers()).isEqualTo(Arrays.asSet(new ChangeId("dummyId")));
		
		AbstractChange dummyUpdate2 = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum2");
			}
		};
		
		// second execution
		LocalChangeRunner testInstance2 = new LocalChangeRunner(Arrays.asList(dummyUpdate2), testInstance.applicationChangeStorage);
		assertThatExceptionOfType(NonCompliantUpdateException.class).isThrownBy(() -> testInstance2.processUpdates());
	}
	
}