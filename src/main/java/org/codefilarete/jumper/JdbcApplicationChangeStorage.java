package org.codefilarete.jumper;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.sql.TransactionSupport;
import org.codefilarete.stalactite.persistence.engine.PersistenceContext;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.binder.LambdaParameterBinder;

/**
 * @author Guillaume Mary
 */
public class JdbcApplicationChangeStorage implements ApplicationChangeStorage {
	
	public static final JumpsTable DEFAULT_STORAGE_TABLE = new JumpsTable();
	
	private final ConnectionProvider connectionProvider;
	
	private JumpsTable storageTable = DEFAULT_STORAGE_TABLE;
	
	public JdbcApplicationChangeStorage(ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
	}
	
	public JumpsTable getStorageTable() {
		return storageTable;
	}
	
	public void setStorageTable(JumpsTable storageTable) {
		this.storageTable = storageTable;
	}
	
	@Override
	public void persist(Change change) {
		HSQLDBDialect dialect = new HSQLDBDialect();
		try (Connection currentConnection = connectionProvider.giveConnection()) {
			TransactionSupport transactionSupport = new TransactionSupport(currentConnection);
			transactionSupport.runAtomically(c -> {
				PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
				
				dialect.getColumnBinderRegistry().register(storageTable.checksum, new LambdaParameterBinder<>(
						(resultSet, columnName) -> new Checksum(resultSet.getString(columnName)),
						(preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toString())));
				persistenceContext.insert(storageTable)
						.set(storageTable.id, change.getIdentifier().toString())
						.set(storageTable.createdAt, LocalDateTime.now())
						.set(storageTable.checksum, change.computeChecksum())
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
	public Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> updates) {
		return null;
	}
	
	/**
	 * Definition of the target table
	 */
	public static class JumpsTable extends Table<JumpsTable> {
		
		public static final String DEFAULT_TABLE_NAME = "JumpsHistory";
		
		public final Column<JumpsTable, String> id = this.addColumn("id", String.class);
		public final Column<JumpsTable, LocalDateTime> createdAt = this.addColumn("createdAt", LocalDateTime.class);
		public final Column<JumpsTable, Checksum> checksum = this.addColumn("checksum", Checksum.class);
		
		public JumpsTable() {
			this(DEFAULT_TABLE_NAME);
		}
		
		public JumpsTable(String tableName) {
			super(tableName);
		}
	}
}
