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

import org.codefilarete.jumper.ApplicationChangeStorage.ChangeSignet;
import org.codefilarete.jumper.ddl.engine.Dialect;
import org.codefilarete.jumper.ddl.engine.ServiceLoaderDialectResolver;
import org.codefilarete.jumper.DialectResolver.DatabaseSignet;
import org.codefilarete.jumper.impl.AbstractJavaChange;
import org.codefilarete.jumper.impl.ChangeChecksumer;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Guillaume Mary
 */
public class ChangeSetRunner {
	
	private final List<Change> changes;
	private final ConnectionProvider connectionProvider;
	private final ApplicationChangeStorage applicationChangeStorage;
	
	private final ExecutionListener executionListener;
	
	private final CachingChangeChecksumer checksumer = new CachingChangeChecksumer();
	
	public ChangeSetRunner(List<Change> changes, ConnectionProvider connectionProvider, ApplicationChangeStorage applicationChangeStorage) {
		this(changes, connectionProvider, applicationChangeStorage, new NoopExecutionListener());
	}
	
	public ChangeSetRunner(List<Change> changes, ConnectionProvider connectionProvider, ApplicationChangeStorage applicationChangeStorage, ExecutionListener executionListener) {
		this.changes = changes;
		this.connectionProvider = connectionProvider;
		this.applicationChangeStorage = applicationChangeStorage;
		this.executionListener = executionListener;
	}
	
	public void processUpdates() throws ExecutionException {
		
		assertNonCompliantUpdates(changes);
		
		Context context = buildContext(connectionProvider.giveConnection());
		List<Change> updatesToRun = keepChangesToRun(changes, context);
		ChangeRunner changeRunner = new ChangeRunner(giveDialect(context.getDatabaseSignet()));
		changeRunner.run(updatesToRun, context);
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
	
	
	private void assertNonCompliantUpdates(List<Change> changes) {
		// NB: we store current update Checksum in a Map to avoid its computation twice
		Map<Change, Checksum> nonCompliantUpdates = new LinkedHashMap<>(changes.size());
		Map<ChangeId, Checksum> currentlyStoredChecksums = applicationChangeStorage.giveChecksum(Iterables.collectToList(changes, Change::getIdentifier));
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
		Set<ChangeId> ranIdentifiers = applicationChangeStorage.giveRanIdentifiers();
		return changes.stream()
				.filter(u -> shouldRun(u, ranIdentifiers, context))
				.collect(Collectors.toList());
	}
	
	/**
	 * Decides whether a {@link Change} must be run
	 *
	 * @param change         the {@link Change} to be checked
	 * @param ranIdentifiers the already ran identifiers
	 * @return true to plan it for running
	 */
	protected boolean shouldRun(Change change, Set<ChangeId> ranIdentifiers, Context context) {
		boolean isAuthorizedToRun = !ranIdentifiers.contains(change.getIdentifier()) || change.shouldAlwaysRun();
		return isAuthorizedToRun && change.shouldRun(context);
	}
	
	@VisibleForTesting
	class ChangeRunner {
		
		private final Logger LOGGER = LoggerFactory.getLogger(ChangeRunner.class);
		
		private final Dialect dialect;
		
		ChangeRunner(Dialect dialect) {
			this.dialect = dialect;
		}
		
		public void run(Iterable<Change> updatesToRun, Context context) throws ExecutionException {
			for (Change change : updatesToRun) {
				executionListener.beforeRun(change);
				run(change, context);
				executionListener.afterRun(change);
				persistState(change);
			}
		}
		
		private void run(Change change, Context context) throws ExecutionException {
			Connection connection = connectionProvider.giveConnection();
			// TODO : handle commit and rollback
			try {
				if (change instanceof AbstractJavaChange) {
					((AbstractJavaChange) change).run(context, connection);
				} else {
					List<String> sqlOrders = dialect.generateScript(change);
					runSqlOrders(sqlOrders, connection);
				}
			} catch (SQLException e) {
				throw new ExecutionException("Error while running change " + change.getIdentifier(), e);
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
			try (Statement statement = connection.createStatement()) {
				String orderType = sqlOrder.trim().substring(0, 6).toLowerCase();
				switch (orderType) {
					case "insert":
					case "update":
					case "delete":
						long updatedRowCount = statement.executeLargeUpdate(sqlOrder);
						LOGGER.info("{} updated rows by " + sqlOrder, updatedRowCount);
						break;
					case "select":    // what's the interest to select something during an update ? not sure this case should be taken into account
						statement.executeQuery(sqlOrder);
						break;
					default:
						// create/alter/drop table, stored procedure execution, grant privileges, ... whatever
						statement.execute(sqlOrder);
				}
			}
		}
		
		private void persistState(Change change) throws ExecutionException {
			try {
				applicationChangeStorage.persist(new ChangeSignet(change.getIdentifier(), checksumer.buildChecksum(change)));
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
