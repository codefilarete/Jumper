package org.codefilarete.jumper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.tool.sql.TransactionSupport;

/**
 * Change that deploys Jumper's history table. Kind of self usage of itself.
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
		dialect.getColumnBinderRegistry().register(storageTable.checksum, new LambdaParameterBinder<>(
				(resultSet, columnName) -> new Checksum(resultSet.getString(columnName)),
				(preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toString())));
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
	public Set<ChangeId> giveRanIdentifiers() {
		return Collections.emptySet();
	}
	
	@Override
	public Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> changes) {
		return null;
	}
	
	protected class ChangeHistoryTableEnsurer extends NoopExecutionListener {
		
		protected Connection connection;
		
		@Override
		public void beforeAll() {
			connection = connectionProvider.giveConnection();
			try {
				ResultSet tables = connection.getMetaData().getTables(storageTable.getSchema().getName(), storageTable.getSchema().getName(), storageTable.getName(), null);
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
			try (Connection ignored = localConnectionProvider.giveConnection()) {
				TransactionSupport.runAtomically(c -> {
					ddlDeployer.deployDDL();
				}, ignored);
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
