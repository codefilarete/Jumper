package org.gama.jumper;

import org.gama.jumper.impl.AbstractJavaUpdate;
import org.gama.jumper.impl.InMemoryApplicationUpdateStorage;
import org.gama.lang.collection.Arrays;
import org.gama.lang.trace.IncrementableInt;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ApplicationUpdateProcessorTest {
	
	@Test
	public void testProcessUpdates_runNonRanUpdates() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		IncrementableInt executionCounter = new IncrementableInt();
		
		InMemoryApplicationUpdateStorage applicationUpdateStorage = new InMemoryApplicationUpdateStorage();
		AbstractUpdate dummyUpdate = new AbstractUpdate("dummyId", false) {
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
		
		// Update must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new UpdateId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Update mustn't be run again because it is marked as not always run
		assertEquals(1, executionCounter.getValue());
	}
	
	@Test
	public void testProcessUpdates_alwaysRun() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		IncrementableInt executionCounter = new IncrementableInt();
		
		InMemoryApplicationUpdateStorage applicationUpdateStorage = new InMemoryApplicationUpdateStorage();
		AbstractUpdate dummyUpdate = new AbstractJavaUpdate("dummyId", true) {
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
		
		// Update must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new UpdateId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		// second execution
		testInstance.processUpdates(Arrays.asList(dummyUpdate), new Context(), applicationUpdateStorage);
		
		// Update must be run again because it is marked as always run
		assertEquals(2, executionCounter.getValue());
	}
	
	@Test(expected = NonCompliantUpdateException.class)
	public void testProcessUpdates_checksumMismatch() {
		ApplicationUpdateProcessor testInstance = new ApplicationUpdateProcessor();
		
		IncrementableInt executionCounter = new IncrementableInt();
		
		InMemoryApplicationUpdateStorage applicationUpdateStorage = new InMemoryApplicationUpdateStorage();
		AbstractUpdate dummyUpdate = new AbstractJavaUpdate("dummyId", true) {
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
		
		// Update must be ran
		assertEquals(1, executionCounter.getValue());
		// id must be stored
		assertEquals(Arrays.asSet(new UpdateId("dummyId")), applicationUpdateStorage.giveRanIdentifiers());
		
		dummyUpdate = new AbstractJavaUpdate("dummyId", true) {
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