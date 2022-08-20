package org.codefilarete.jumper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Instant;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.sql.TransactionSupport;

public class JdbcUpdateProcessLockStorage implements UpdateProcessLockStorage {
	
	private static final String HOSTNAME;
	
	static {
		try {
			HOSTNAME = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static final UpdateProcessLockTable DEFAULT_STORAGE_TABLE = new UpdateProcessLockTable();
	
	private final ConnectionProvider connectionProvider;
	
	private final PersistenceContext persistenceContext;
	
	private final UpdateProcessLockTable storageTable;
	
	private final LockTableEnsurer lockTableEnsurer;
	
	public JdbcUpdateProcessLockStorage(ConnectionProvider connectionProvider) {
		this(connectionProvider, DEFAULT_STORAGE_TABLE);
	}
	
	public JdbcUpdateProcessLockStorage(ConnectionProvider connectionProvider, Dialect dialect) {
		this(connectionProvider, DEFAULT_STORAGE_TABLE, dialect);
	}
	
	public JdbcUpdateProcessLockStorage(ConnectionProvider connectionProvider, UpdateProcessLockTable lockTable) {
		this.connectionProvider = connectionProvider;
		this.persistenceContext = new PersistenceContext(connectionProvider);
		this.storageTable = lockTable;
		this.lockTableEnsurer = new LockTableEnsurer();
	}
	
	public JdbcUpdateProcessLockStorage(ConnectionProvider connectionProvider, UpdateProcessLockTable lockTable, Dialect dialect) {
		this.connectionProvider = connectionProvider;
		this.persistenceContext = new PersistenceContext(connectionProvider, dialect);
		this.storageTable = lockTable;
		this.lockTableEnsurer = new LockTableEnsurer();
	}
	
	public LockTableEnsurer getLockTableEnsurer() {
		return lockTableEnsurer;
	}
	
	@Override
	public void insertRow(String lockIdentifier) {
		try (Connection currentConnection = connectionProvider.giveConnection()) {
			TransactionSupport transactionSupport = new TransactionSupport(currentConnection);
			transactionSupport.runAtomically(c -> {
				persistenceContext.insert(storageTable)
						.set(storageTable.id, lockIdentifier)
						.set(storageTable.createdAt, Instant.now())
						.set(storageTable.createdBy, HOSTNAME)
						.execute();
			});
		} catch (SQLExecutionException e) {
			if (Exceptions.findExceptionInCauses(e, SQLIntegrityConstraintViolationException.class) != null) {
				Duo<String, Instant> presentLock = persistenceContext.select(Duo::new, storageTable.createdBy, storageTable.createdAt).get(0);
				throw new RuntimeException("Can't obtain lock to process changes : a lock is already acquired by " + presentLock.getLeft() + " since " + presentLock.getRight());
			} else {
				throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void deleteRow(String lockIdentifier) {
		Connection currentConnection = connectionProvider.giveConnection();
		TransactionSupport transactionSupport = new TransactionSupport(currentConnection);
		try {
			transactionSupport.runAtomically(c -> {
				persistenceContext.delete(storageTable).where(storageTable.id, Operators.eq(lockIdentifier))
						.execute();
			});
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected class LockTableEnsurer extends NoopExecutionListener {
		
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
	
	public static class UpdateProcessLockTable extends Table<UpdateProcessLockTable> {
		
		public static final String DEFAULT_TABLE_NAME = "UpdateProcessLockTable";
		
		public final Column<UpdateProcessLockTable, String> id;
		public final Column<UpdateProcessLockTable, Instant> createdAt;
		public final Column<UpdateProcessLockTable, String> createdBy;
		
		public UpdateProcessLockTable() {
			this(DEFAULT_TABLE_NAME, "id", "createdAt", "createdBy");
		}
		
		public UpdateProcessLockTable(String name, String idColumnName, String createdAtColumnName, String createdByColumnName) {
			super(name);
			this.id = this.addColumn(idColumnName, String.class)
					.primaryKey();
			this.createdAt = this.addColumn(createdAtColumnName, Instant.class)
					.nullable(false);
			this.createdBy = this.addColumn(createdByColumnName, String.class)
					.nullable(false);
			
		}
	}
}
