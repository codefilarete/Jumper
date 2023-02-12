package org.codefilarete.jumper.impl;

import org.codefilarete.jumper.ChangeSetId;
import org.codefilarete.jumper.ChangeStorage;
import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.NoopExecutionListener;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.sql.TransactionSupport;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Change that deploys Jumper's history table.
 *
 * @author Guillaume Mary
 */
public class JdbcChangeStorage implements ChangeStorage {
	
	public static final ChangeHistoryTable DEFAULT_STORAGE_TABLE = new ChangeHistoryTable();
	
	private final ConnectionProvider connectionProvider;
	private final PersistenceContext persistenceContext;
	private ChangeHistoryTable storageTable = DEFAULT_STORAGE_TABLE;
	private final ChangeHistoryTableEnsurer changeHistoryTableEnsurer;
	
	public JdbcChangeStorage(ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
		// we use Stalactite to perform inserts of Checksums
		Dialect dialect = new ServiceLoaderDialectResolver().determineDialect(connectionProvider.giveConnection());
		// we register a Binder for reading and writing a Checksum in the configured database column as a String
		dialect.getColumnBinderRegistry().register(Checksum.class, new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Checksum::new, Checksum::toString));
		dialect.getSqlTypeRegistry().put(Checksum.class, "VARCHAR(255)");
		persistenceContext = new PersistenceContext(connectionProvider, dialect);
		changeHistoryTableEnsurer = new ChangeHistoryTableEnsurer();
	}
	
	public ChangeHistoryTableEnsurer getChangeHistoryTableEnsurer() {
		return changeHistoryTableEnsurer;
	}
	
	public ChangeHistoryTable getStorageTable() {
		return storageTable;
	}
	
	public void setStorageTable(ChangeHistoryTable storageTable) {
		this.storageTable = storageTable;
	}
	
	@Override
	public void persist(ChangeSignet change) {
		Connection currentConnection = connectionProvider.giveConnection();
		TransactionSupport transactionSupport = new TransactionSupport(currentConnection);
		try {
			transactionSupport.runAtomically(c -> {
				persistenceContext.insert(storageTable)
						.set(storageTable.id, change.getChangeId().toString())
						.set(storageTable.createdAt, LocalDateTime.now())
						.set(storageTable.checksum, change.getChecksum())
						.execute();
			});
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Set<ChangeSetId> giveRanIdentifiers() {
		return new HashSet<>(persistenceContext.newQuery(QueryEase.select(storageTable.id)
						.from(storageTable), ChangeSetId.class)
				.mapKey(ChangeSetId::new, storageTable.id)
				.execute());
	}
	
	@Override
	public Map<ChangeSetId, Checksum> giveChecksum(Iterable<ChangeSetId> changes) {
		List<Duo> changeIds = persistenceContext.newQuery(QueryEase.select(storageTable.id, storageTable.checksum)
						.from(storageTable).where(storageTable.id, Operators.in(Iterables.collectToList(changes, ChangeSetId::toString))), Duo.class)
				.mapKey(Duo::new, storageTable.id, storageTable.checksum)
				.execute();
		return Iterables.map((List<Duo<String, Checksum>>) (List) changeIds, duo -> new ChangeSetId(duo.getLeft()), Duo::getRight);
	}
	
	protected class ChangeHistoryTableEnsurer extends NoopExecutionListener {
		
		protected Connection connection;
		
		@Override
		public void beforeProcess() {
			connection = connectionProvider.giveConnection();
			try {
				ResultSet tables = connection.getMetaData().getTables(
						Nullable.nullable(storageTable.getSchema()).map(Schema::getName).get(),
						Nullable.nullable(storageTable.getSchema()).map(Schema::getName).get(),
						storageTable.getName(),
						null);
				if (!tables.next()) {
					createTable();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		
		protected void createTable() {
			// we register a Binder for reading and writing a Checksum in the configured database column as a String
			Dialect dialect = persistenceContext.getDialect();
			SimpleConnectionProvider localConnectionProvider = new SimpleConnectionProvider(connection);
			DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), localConnectionProvider);
			ddlDeployer.getDdlGenerator().addTables(storageTable);
			Connection connection = localConnectionProvider.giveConnection();
			try {
				TransactionSupport.runAtomically(c -> {
					ddlDeployer.deployDDL();
				}, connection);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 * Definition of the target table
	 */
	public static class ChangeHistoryTable extends Table<ChangeHistoryTable> {
		
		public static final String DEFAULT_TABLE_NAME = "ChangeHistoryTable";
		
		public final Column<ChangeHistoryTable, String> id;
		public final Column<ChangeHistoryTable, LocalDateTime> createdAt;
		public final Column<ChangeHistoryTable, Checksum> checksum;
		
		public ChangeHistoryTable() {
			this(DEFAULT_TABLE_NAME, "id", "createdAt", "checksum");
		}
		
		public ChangeHistoryTable(String tableName, String idColumnName, String createdAtColumnName, String checksumColumName) {
			super(tableName);
			id = this.addColumn(idColumnName, String.class)
					.primaryKey()
					.autoGenerated();
			createdAt = this.addColumn(createdAtColumnName, LocalDateTime.class)
					.nullable(false);
			checksum = this.addColumn(checksumColumName, Checksum.class)
					.nullable(false);
		}
	}
}
