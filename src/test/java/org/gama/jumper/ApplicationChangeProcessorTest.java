package org.gama.jumper;

import org.gama.jumper.impl.AbstractJavaChange;
import org.gama.jumper.impl.InMemoryApplicationChangeStorage;
import org.gama.lang.collection.Arrays;
import org.gama.lang.trace.IncrementableInt;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ApplicationChangeProcessorTest {
	
	@Test
	public void testProcessUpdates_runNonRanUpdates() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		IncrementableInt executionCounter = new IncrementableInt();
		
		InMemoryApplicationChangeStorage applicationUpdateStorage = new InMemoryApplicationChangeStorage();
		AbstractChange dummyUpdate = new AbstractChange("dummyId", false) {
			@Override
			public void run() throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Change must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new ChangeId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Change mustn't be run again because it is marked as not always run
		assertEquals(1, executionCounter.getValue());
	}
	
	@Test
	public void testProcessUpdates_alwaysRun() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		IncrementableInt executionCounter = new IncrementableInt();
		
		InMemoryApplicationChangeStorage applicationUpdateStorage = new InMemoryApplicationChangeStorage();
		AbstractChange dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run() throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Change must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new ChangeId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Change must be run again because it is marked as always run
		assertEquals(2, executionCounter.getValue());
	}
	
	@Test(expected = NonCompliantUpdateException.class)
	public void testProcessUpdates_checksumMismatch() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		IncrementableInt executionCounter = new IncrementableInt();
		
		InMemoryApplicationChangeStorage applicationUpdateStorage = new InMemoryApplicationChangeStorage();
		AbstractChange dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run() throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Change must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new ChangeId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		dummyUpdate = new AbstractJavaChange("dummyId", true) {
			@Override
			public void run() throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum2");
			}
		};
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
	}
	
}