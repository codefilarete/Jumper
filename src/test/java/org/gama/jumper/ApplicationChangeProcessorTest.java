package org.codefilarete.jumper;

import org.codefilarete.jumper.impl.AbstractJavaChange;
import org.codefilarete.jumper.impl.InMemoryApplicationChangeStorage;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class ApplicationChangeProcessorTest {
	
	@Test
	public void testProcessUpdates_runNonRanUpdates() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		InMemoryApplicationChangeStorage applicationUpdateStorage = new InMemoryApplicationChangeStorage();
		AbstractChange dummyUpdate = new AbstractChange("dummyId", false) {
			@Override
			public void run(Context context) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(null, null), applicationUpdateStorage);
		
		// Change must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new ChangeId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(null, null), applicationUpdateStorage);
		
		// Change mustn't be run again because it is marked as not always run
		assertEquals(1, executionCounter.getValue());
	}
	
	@Test
	public void testProcessUpdates_alwaysRun() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		InMemoryApplicationChangeStorage applicationUpdateStorage = new InMemoryApplicationChangeStorage();
		AbstractChange dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run(Context context) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(null, null), applicationUpdateStorage);
		
		// Change must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new ChangeId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(null, null), applicationUpdateStorage);
		
		// Change must be run again because it is marked as always run
		assertEquals(2, executionCounter.getValue());
	}
	
	@Test
	public void testProcessUpdates_checksumMismatch() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		InMemoryApplicationChangeStorage applicationUpdateStorage = new InMemoryApplicationChangeStorage();
		AbstractChange dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run(Context context) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(null, null), applicationUpdateStorage);
		
		// Change must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new ChangeId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		AbstractChange dummyUpdate2 = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run(Context context) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum2");
			}
		};
		
		// second execution
		assertThrows(NonCompliantUpdateException.class, () -> testInstance.processUpdates(Arrays.asList(dummyUpdate2), new Context(null, null), applicationUpdateStorage));
	}
	
}