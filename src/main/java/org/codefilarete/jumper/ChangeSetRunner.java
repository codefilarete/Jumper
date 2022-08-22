package org.codefilarete.jumper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.jumper.ChangeStorage.ChangeSignet;
import org.codefilarete.jumper.DialectResolver.DatabaseSignet;
import org.codefilarete.jumper.ddl.engine.Dialect;
import org.codefilarete.jumper.ddl.engine.ServiceLoaderDialectResolver;
import org.codefilarete.jumper.impl.AbstractJavaChange;
import org.codefilarete.jumper.impl.ChangeChecksumer;
import org.codefilarete.jumper.impl.DDLChange;
import org.codefilarete.jumper.impl.SQLChange;
import org.codefilarete.jumper.impl.StringChecksumer;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.sql.TransactionSupport;

/**
 * @author Guillaume Mary
 */
public class ChangeSetRunner {
	
	public static ChangeSetRunner forJdbcStorage(ConnectionProvider connectionProvider, Change... changes) {
		JdbcChangeStorage changeHistoryStorage = new JdbcChangeStorage(connectionProvider);
		JdbcUpdateProcessLockStorage processLockStorage = new JdbcUpdateProcessLockStorage(connectionProvider);
		ChangeSetRunner result = new ChangeSetRunner(Arrays.asList(changes), connectionProvider, changeHistoryStorage, processLockStorage);
		// we add storage listeners so they can create their tables at very beginning of the process
		result.addExecutionListener(processLockStorage.getLockTableEnsurer());
		result.addExecutionListener(changeHistoryStorage.getChangeHistoryTableEnsurer());
		return result;
	}
	
	private final List<Change> changes;
	private final ConnectionProvider connectionProvider;
	private final ChangeStorage changeStorage;
	private final UpdateProcessLockStorage processLockStorage;
	
	private final ChangeSetExecutionListenerCollection executionListener = new ChangeSetExecutionListenerCollection();
	
	private final CachingChangeChecksumer checksumer = new CachingChangeChecksumer();
	
	/**
	 * {@link Connection} to use during change execution process (because we handle transaction on it)
	 */
	private Connection executionConnection;
	
	public ChangeSetRunner(List<Change> changes, ConnectionProvider connectionProvider, ChangeStorage changeStorage, UpdateProcessLockStorage lockStorage) {
		this.changes = changes;
		this.connectionProvider = connectionProvider;
		this.changeStorage = changeStorage;
		this.processLockStorage = lockStorage;
	}
	
	public void addExecutionListener(ChangeSetExecutionListener executionListener) {
		this.executionListener.add(executionListener);
	}
	
	public void processUpdate() throws ExecutionException {
		executionListener.beforeProcess();
		// note that we decouple lock acquisition from try-with-resource to avoid closing Lock on acquisition error
		UpdateLock voluntarilyOutOfTryWithResource = acquireUpdateLock();
		try (UpdateLock ignored = voluntarilyOutOfTryWithResource) {
			assertNonCompliantChanges();
			
			executionConnection = connectionProvider.giveConnection();
			
			Context context = buildContext(executionConnection);
			List<Change> updatesToRun = keepChangesToRun(changes, context);
			ChangeRunner changeRunner = new ChangeRunner(giveDialect(context.getDatabaseSignet()));
			changeRunner.run(updatesToRun, context);
		}
		executionListener.afterProcess();
	}
	
	private UpdateLock acquireUpdateLock() {
		String allChangesSignature = changes.stream().map(checksumer::buildChecksum).map(Object::toString).collect(Collectors.toList()).toString();
		Checksum allChangesChecksum = new StringChecksumer().checksum(allChangesSignature);
		processLockStorage.insertRow(allChangesChecksum.toString());
		return new UpdateLock(allChangesChecksum.toString());
	}
	
	private class UpdateLock implements AutoCloseable {
		
		private final String identifier;
		
		public UpdateLock(String identifier) {
			this.identifier = identifier;
		}
		
		@Override
		public void close() {
			releaseUpdateLock();
		}
		
		private void releaseUpdateLock() {
			processLockStorage.deleteRow(identifier);
		}
	}
	
	protected Context buildContext(Connection connection) {
		return new Context(DialectResolver.DatabaseSignet.fromMetadata(connection));
	}
	
	/**
	 * To be overridden to create your own {@link Dialect} if generated SQL by default one doesn't fit your needs.
	 *
	 * @param databaseSignet database vendor and version
	 * @return Dialect to use for SQL applied on database
	 */
	protected Dialect giveDialect(DatabaseSignet databaseSignet) {
		return new ServiceLoaderDialectResolver().determineDialect(databaseSignet);
	}
	
	private void assertNonCompliantChanges() {
		// NB: we store current update Checksum in a Map to avoid its computation twice
		Map<Change, Checksum> nonCompliantUpdates = new LinkedHashMap<>(changes.size());
		Map<ChangeId, Checksum> currentlyStoredChecksums = changeStorage.giveChecksum(Iterables.collectToList(changes, Change::getIdentifier));
		changes.forEach(change -> {
			Checksum currentlyStoredChecksum = currentlyStoredChecksums.get(change.getIdentifier());
			if (currentlyStoredChecksum != null) {
				Checksum currentChecksum = checksumer.buildChecksum(change);
				if (!currentlyStoredChecksum.equals(currentChecksum)
						|| change.getCompatibleChecksums().contains(currentChecksum)) {
					nonCompliantUpdates.put(change, currentlyStoredChecksum);
				}
			}
		});
		if (!nonCompliantUpdates.isEmpty()) {
			throw new NonCompliantUpdateException("Some changes have changed since last run. Add a compatible signature or review conflict",
					nonCompliantUpdates);
		}
	}
	
	private List<Change> keepChangesToRun(List<Change> changes, Context context) {
		Set<ChangeId> ranIdentifiers = changeStorage.giveRanIdentifiers();
		return changes.stream()
				.filter(u -> shouldRun(u, ranIdentifiers, context))
				.collect(Collectors.toList());
	}
	
	/**
	 * Decides whether a {@link Change} must be run
	 *
	 * @param change the {@link Change} to be checked
	 * @param ranIdentifiers the already ran identifiers
	 * @return true to plan it for running
	 */
	protected boolean shouldRun(Change change, Set<ChangeId> ranIdentifiers, Context context) {
		boolean isAuthorizedToRun = !ranIdentifiers.contains(change.getIdentifier()) || change.shouldAlwaysRun();
		return isAuthorizedToRun && change.shouldRun(context);
	}
	
	@VisibleForTesting
	class ChangeRunner {
		
		private final Dialect dialect;
		
		ChangeRunner(Dialect dialect) {
			this.dialect = dialect;
		}
		
		public void run(Iterable<Change> updatesToRun, Context context) throws ExecutionException {
			for (Change change : updatesToRun) {
				executionListener.beforeRun(change);
				try {
					TransactionSupport.runAtomically(c -> {
						run(change, context);
						persistState(change);
					}, executionConnection);
				} catch (SQLException e) {
					throw new ExecutionException("Error while running change " + change.getIdentifier(), e);
				}
				executionListener.afterRun(change);
			}
		}
		
		private void run(Change change, Context context) throws SQLException {
			try {
				if (change instanceof AbstractJavaChange) {
					((AbstractJavaChange) change).run(context, executionConnection);
				} else {
					List<String> sqlOrders;
					if (change instanceof SQLChange) {
						sqlOrders = ((SQLChange) change).getSqlOrders();
					} else if (change instanceof DDLChange) {
						sqlOrders = dialect.generateScript((DDLChange) change);
					} else {
						throw new NotImplementedException("Change of type " + Reflections.toString(change.getClass()) + " is not supported");
					}
					runSqlOrders(sqlOrders, executionConnection);
				}
			} catch (SQLException e) {
				throw new SQLException("Error while running change " + change.getIdentifier(), e);
			}
		}
		
		/**
		 * Executes sql statements within given transaction
		 *
		 * @param sqlOrders sql orders to be executed
		 * @param connection the {@link Connection} on which to run sql orders
		 * @throws SQLException any error thrown by sql orders or transaction management
		 */
		private void runSqlOrders(List<String> sqlOrders, Connection connection) throws SQLException {
			for (String sqlOrder : sqlOrders) {
				try {
					runSqlOrder(sqlOrder, connection);
				} catch (SQLException e) {
					throw new SQLException("Error executing " + sqlOrder, e);
				}
			}
		}
		
		private void runSqlOrder(String sqlOrder, Connection connection) throws SQLException {
			Long updatedRowCount = null;
			try (Statement statement = connection.createStatement()) {
				String orderType = sqlOrder.trim().substring(0, 6).toLowerCase();
				switch (orderType) {
					case "insert":
					case "update":
					case "delete":
						updatedRowCount = statement.executeLargeUpdate(sqlOrder);
						break;
					case "select":    // what's the interest to select something during an update ? not sure this case should be taken into account
						statement.executeQuery(sqlOrder);
						break;
					default:
						// create/alter/drop table, stored procedure execution, grant privileges, ... whatever
						statement.execute(sqlOrder);
				}
			}
			executionListener.afterRun(sqlOrder, updatedRowCount);
		}
		
		private void persistState(Change change) throws ExecutionException {
			try {
				changeStorage.persist(new ChangeSignet(change.getIdentifier(), checksumer.buildChecksum(change)));
			} catch (RuntimeException | OutOfMemoryError e) {
				throw new ExecutionException("State of change " + change.getIdentifier() + " couldn't be stored", e);
			}
		}
	}
	
	private static class CachingChangeChecksumer extends ChangeChecksumer {
		
		private final Map<Change, Checksum> checksumCache = new HashMap<>(50);
		
		@Override
		public Checksum buildChecksum(Change change) {
			return checksumCache.computeIfAbsent(change, super::buildChecksum);
		}
	}
}
