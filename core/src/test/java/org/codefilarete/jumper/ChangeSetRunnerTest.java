package org.codefilarete.jumper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.jumper.ChangeStorage.ChangeSignet;
import org.codefilarete.jumper.DialectResolver.DatabaseSignet;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.engine.Dialect;
import org.codefilarete.jumper.impl.AbstractJavaChange;
import org.codefilarete.jumper.impl.SQLChange;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class ChangeSetRunnerTest {
	
	private static class LocalChangeRunner extends ChangeSetRunner {
		
		private final Context context = new Context(new DatabaseSignet("", 1, 0));
		
		private final ChangeStorage changeStorage;
		
		private final Dialect dialect = new Dialect();
		
		private LocalChangeRunner(List<ChangeSet> changes, ConnectionProvider connectionProvider, ChangeStorage changeStorage) {
			super(changes, connectionProvider, changeStorage, new UpdateProcessLockStorage() {
				@Override
				public void insertRow(String lockIdentifier) {
				
				}
				
				@Override
				public void deleteRow(String lockIdentifier) {
				
				}
			});
			this.changeStorage = changeStorage;
		}
		
		@Override
		protected Context buildContext(Connection connection) {
			return context;
		}
		
		@Override
		protected Dialect giveDialect(DatabaseSignet databaseSignet) {
			return dialect;
		}
	}
	
	@Test
	void processUpdate_runNonRanUpdates() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		AbstractJavaChange dummyUpdate = new AbstractJavaChange() {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		LocalChangeRunner testInstance = new LocalChangeRunner(Arrays.asList(new ChangeSet("dummyId").addChanges(dummyUpdate)), () -> connectionMock, new InMemoryChangeStorage());
		testInstance.processUpdate();
		
		// Change must be ran
		assertThat(executionCounter.getValue()).isEqualTo(1);
		// id must be stored
		assertThat(testInstance.changeStorage.giveRanIdentifiers()).isEqualTo(Arrays.asSet(new ChangeSetId("dummyId")));
		
		// second execution
		testInstance.processUpdate();
		
		// Change mustn't be run again because it is marked as not always run
		assertThat(executionCounter.getValue()).isEqualTo(1);
	}
	
	@Test
	void processUpdate_changeIsMarkedAsAlwaysRun_runAlways() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		Change dummyUpdate = new AbstractJavaChange() {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		LocalChangeRunner testInstance = new LocalChangeRunner(Arrays.asList(new ChangeSet("dummyId").alwaysRun(true).addChanges(dummyUpdate)), () -> connectionMock, new InMemoryChangeStorage());
		testInstance.processUpdate();
		
		// Change must be ran
		assertThat(executionCounter.getValue()).isEqualTo(1);
		// id must be stored
		assertThat(testInstance.changeStorage.giveRanIdentifiers()).isEqualTo(Arrays.asSet(new ChangeSetId("dummyId")));
		
		// second execution
		testInstance.processUpdate();
		
		// Change must be run again because it is marked as always run
		assertThat(executionCounter.getValue()).isEqualTo(2);
	}
	
	@Test
	void processUpdate_checksumMismatch() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		
		ModifiableInt executionCounter = new ModifiableInt();
		
		Change dummyUpdate = new AbstractJavaChange() {
			@Override
			public void run(Context context, Connection connection) throws ExecutionException {
				executionCounter.increment();
			}
			
			@Override
			public Checksum computeChecksum() {
				return new Checksum("dummyChecksum");
			}
		};
		
		LocalChangeRunner testInstance = new LocalChangeRunner(Arrays.asList(new ChangeSet("dummyId").addChanges(dummyUpdate)), () -> connectionMock, new InMemoryChangeStorage());
		testInstance.processUpdate();
		
		// Change must be ran
		assertThat(executionCounter.getValue()).isEqualTo(1);
		// id must be stored
		assertThat(testInstance.changeStorage.giveRanIdentifiers()).isEqualTo(Arrays.asSet(new ChangeSetId("dummyId")));
		
		Change dummyUpdate2 = new AbstractJavaChange() {
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
		LocalChangeRunner testInstance2 = new LocalChangeRunner(Arrays.asList(new ChangeSet("dummyId").addChanges(dummyUpdate2)), () -> connectionMock, testInstance.changeStorage);
		assertThatExceptionOfType(NonCompliantUpdateException.class).isThrownBy(testInstance2::processUpdate);
	}
	
	@Test
	void processUpdate_changeIsPersisted() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		ChangeSet change = new ChangeSet("dummyId").addChanges(new DropTable("toto"));
		ModifiableInt beforeRunCount = new ModifiableInt();
		ModifiableInt afterRunCount = new ModifiableInt();
		LocalChangeRunner localChangeRunner = new LocalChangeRunner(Arrays.asList(change), () -> connectionMock, new InMemoryChangeStorage());
		localChangeRunner.addExecutionListener(new NoopExecutionListener() {
			@Override
			public void beforeRun(ChangeSet changes) {
				beforeRunCount.increment();
			}
			
			@Override
			public void afterRun(ChangeSet changes) {
				afterRunCount.increment();
			}
		});
		localChangeRunner.processUpdate();
		assertThat(beforeRunCount.getValue()).isEqualTo(1);
		assertThat(afterRunCount.getValue()).isEqualTo(1);
	}
	
	@Test
	void processUpdate_listenerIsCalled() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		Holder<ChangeSignet> changeSignetCapturer = new Holder<>();
		ChangeSet change = new ChangeSet("x").addChanges(new DropTable("toto"));
		LocalChangeRunner localChangeRunner = new LocalChangeRunner(Arrays.asList(change), () -> connectionMock, new ChangeStorage() {
			@Override
			public void persist(ChangeSignet change) {
				changeSignetCapturer.set(change);
			}
			
			@Override
			public Set<ChangeSetId> giveRanIdentifiers() {
				// nothing already run
				return Collections.emptySet();
			}
			
			@Override
			public Map<ChangeSetId, Checksum> giveChecksum(Iterable<ChangeSetId> changes) {
				// nothing already run
				return new HashMap<>();
			}
		});
		localChangeRunner.processUpdate();
		assertThat(changeSignetCapturer.get().getChangeId().toString()).isEqualTo("x");
	}
	
	@Test
	void processUpdate_withJavaChange_JavaChangeRunMethodIsCalled() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		Holder<Context> contextCapturer = new Holder<>();
		Holder<Connection> connectionCapturer = new Holder<>();
		ChangeSet change = new ChangeSet("dummyId").addChanges(new AbstractJavaChange() {
			@Override
			public void run(Context context, Connection connection) {
				contextCapturer.set(context);
				connectionCapturer.set(connection);
			}
		});
		LocalChangeRunner localChangeRunner = new LocalChangeRunner(Arrays.asList(change), () -> connectionMock, new InMemoryChangeStorage());
		localChangeRunner.processUpdate();
		assertThat(contextCapturer.get()).isEqualTo(localChangeRunner.context);
		assertThat(connectionCapturer.get()).isEqualTo(connectionMock);
	}
	
	static Object[][] processUpdate_withCRUDChange_adhocStatementMethodIsCalled() throws NoSuchMethodException {
		return new Object[][] {
				{ new SQLChange("insert ..."), PreparedStatement.class.getMethod("executeLargeUpdate", String.class) },
				{ new SQLChange("update ..."), PreparedStatement.class.getMethod("executeLargeUpdate", String.class) },
				{ new SQLChange("delete ..."), PreparedStatement.class.getMethod("executeLargeUpdate", String.class) },
				{ new SQLChange("select ..."), PreparedStatement.class.getMethod("executeQuery", String.class) }
		};
	}
	
	@ParameterizedTest
	@MethodSource
	void processUpdate_withCRUDChange_adhocStatementMethodIsCalled(SQLChange sqlChange, Method method) throws SQLException, InvocationTargetException, IllegalAccessException {
		Connection connectionMock = Mockito.mock(Connection.class);
		PreparedStatement preparedStatementMock = Mockito.mock(PreparedStatement.class);
		when(connectionMock.createStatement()).thenReturn(preparedStatementMock);
		LocalChangeRunner localChangeRunner = new LocalChangeRunner(Arrays.asList(new ChangeSet("dummyId").addChanges(sqlChange)), () -> connectionMock, new InMemoryChangeStorage());
		localChangeRunner.processUpdate();
		method.invoke(verify(preparedStatementMock), sqlChange.getSqlOrders().get(0));
	}
	
	@Test
	void processUpdate_acquireAndReleaseLock() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		when(connectionMock.createStatement()).thenReturn(Mockito.mock(PreparedStatement.class));
		Holder<Context> contextCapturer = new Holder<>();
		Holder<Connection> connectionCapturer = new Holder<>();
		ChangeSet change = new ChangeSet("dummyId").addChanges(new AbstractJavaChange() {
			@Override
			public void run(Context context, Connection connection) {
				contextCapturer.set(context);
				connectionCapturer.set(connection);
			}
		});
		LocalChangeRunner localChangeRunner = new LocalChangeRunner(Arrays.asList(change), () -> connectionMock, new InMemoryChangeStorage());
		localChangeRunner.processUpdate();
		assertThat(contextCapturer.get()).isEqualTo(localChangeRunner.context);
		assertThat(connectionCapturer.get()).isEqualTo(connectionMock);
	}
	
}