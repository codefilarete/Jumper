package org.gama.jumper;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import org.gama.lang.sql.TransactionSupport;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class JdbcApplicationChangeStorage implements ApplicationChangeStorage {
	
	static final Table TABLE_STORAGE = new Table("JumpsHistory");
	static final Column<String> id = TABLE_STORAGE.addColumn("id", String.class);
	static final Column<LocalDateTime> createdAt = TABLE_STORAGE.addColumn("createdAt", LocalDateTime.class);
	static final Column<Checksum> checksum = TABLE_STORAGE.addColumn("checksum", Checksum.class);
	
	private final ConnectionProvider connectionProvider;
	
	public JdbcApplicationChangeStorage(ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
	}
	
	
	@Override
	public void persist(Change change) {
		HSQLDBDialect dialect = new HSQLDBDialect();
		try (Connection currentConnection = connectionProvider.getCurrentConnection()) {
			TransactionSupport transactionSupport = new TransactionSupport(currentConnection);
			transactionSupport.runAtomically(c -> {
				PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
				
				dialect.getColumnBinderRegistry().register(checksum, new LambdaParameterBinder<>(
						(resultSet, columnName) -> new Checksum(resultSet.getString(columnName)),
						(preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toString())));
				persistenceContext.insert(TABLE_STORAGE)
						.set(id, change.getIdentifier().toString())
						.set(createdAt, LocalDateTime.now())
						.set(checksum, change.computeChecksum())
						.execute();
			});
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Set<ChangeId> giveRanIdentifiers() {
		return null;
	}
	
	@Override
	public Map<ChangeId, Checksum> giveChecksum(Iterable<ChangeId> updates) {
		return null;
	}
}
