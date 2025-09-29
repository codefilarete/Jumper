package org.codefilarete.jumper.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Instant;

import org.codefilarete.jumper.NoopExecutionListener;
import org.codefilarete.jumper.SeparateConnectionProvider;
import org.codefilarete.jumper.UpdateProcessSemaphore;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.sql.TransactionSupport;

/**
 * A class that handles update lock through an SQL row.
 *
 * Persistence is based on Stalactite.
 *
 * @author Guillaume Mary
 */
public class JdbcUpdateProcessSemaphore implements UpdateProcessSemaphore {
	
	private static final String HOSTNAME;
	
	static {
		try {
			HOSTNAME = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static final UpdateProcessLockTable DEFAULT_STORAGE_TABLE = new UpdateProcessLockTable();
	
	private final SeparateConnectionProvider connectionProvider;
	
	private final PersistenceContext persistenceContext;
	
	private final UpdateProcessLockTable storageTable;
	
	private final LockTableEnsurer lockTableEnsurer;
	
	public JdbcUpdateProcessSemaphore(SeparateConnectionProvider connectionProvider) {
		this(connectionProvider, DEFAULT_STORAGE_TABLE);
	}
	
	public JdbcUpdateProcessSemaphore(SeparateConnectionProvider connectionProvider, Dialect dialect) {
		this(connectionProvider, DEFAULT_STORAGE_TABLE, dialect);
	}
	
	public JdbcUpdateProcessSemaphore(SeparateConnectionProvider connectionProvider, UpdateProcessLockTable lockTable) {
		this.connectionProvider = connectionProvider;
		this.persistenceContext = new PersistenceContext(connectionProvider);
		this.storageTable = lockTable;
		this.lockTableEnsurer = new LockTableEnsurer();
	}
	
	public JdbcUpdateProcessSemaphore(SeparateConnectionProvider connectionProvider, UpdateProcessLockTable lockTable, Dialect dialect) {
		this.connectionProvider = connectionProvider;
		this.persistenceContext = new PersistenceContext(connectionProvider, dialect);
		this.storageTable = lockTable;
		this.lockTableEnsurer = new LockTableEnsurer();
	}
	
	public LockTableEnsurer getLockTableEnsurer() {
		return lockTableEnsurer;
	}
	
	@Override
	public void acquireLock(String lockIdentifier) {
		Connection currentConnection = connectionProvider.giveSeparateConnection();
		try {
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
				Duo<String, Instant> presentLock = persistenceContext.select(Duo::new, storageTable.createdBy, storageTable.createdAt).stream().findFirst().orElse(null);
				throw new RuntimeException("Can't obtain lock to process changes : a lock is already acquired by " + presentLock.getLeft() + " since " + presentLock.getRight());
			} else {
				throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void releaseLock(String lockIdentifier) {
		Connection currentConnection = connectionProvider.giveSeparateConnection();
		TransactionSupport transactionSupport = new TransactionSupport(currentConnection);
		try {
			transactionSupport.runAtomically(c -> {
				persistenceContext.delete(storageTable, new Where<>(storageTable.id, Operators.eq(lockIdentifier)))
						.execute();
			});
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public class LockTableEnsurer extends NoopExecutionListener {
		
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
			DDLDeployer ddlDeployer = new DDLDeployer(dialect, localConnectionProvider);
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
	 * Description of the table containing locking row.
	 *
	 * @author Guillaume Mary
	 */
	public static class UpdateProcessLockTable extends Table<UpdateProcessLockTable> {
		
		public static final String DEFAULT_TABLE_NAME = "UpdateProcessLockTable";
		
		public final Column<UpdateProcessLockTable, String> id;
		/** Creation instant of the lock, for information only (may help debug) */
		public final Column<UpdateProcessLockTable, Instant> createdAt;
		/** Creator of the lock, for information only (may help debug) */
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
