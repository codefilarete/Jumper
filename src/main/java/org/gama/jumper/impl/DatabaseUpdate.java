package org.gama.jumper.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

import org.gama.jumper.AbstractUpdate;
import org.gama.jumper.UpdateId;
import org.gama.lang.sql.TransactionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Guillaume Mary
 */
public class DatabaseUpdate extends AbstractUpdate {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseUpdate.class);
	
	private final DataSource dataSource;
	private final String[] sqlOrders;
	
	public DatabaseUpdate(UpdateId updateId, boolean shouldAlwaysRun, DataSource dataSource, String[] sqlOrders) {
		super(updateId, shouldAlwaysRun);
		this.dataSource = dataSource;
		this.sqlOrders = sqlOrders;
	}
	
	public DatabaseUpdate(String identifier, boolean shouldAlwaysRun, DataSource dataSource, String[] sqlOrders) {
		this(new UpdateId(identifier), shouldAlwaysRun, dataSource, sqlOrders);
	}
	
	@Override
	public void run() throws ExecutionException {
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			throw new ExecutionException("Can't get connection from datasource", e);
		}
		try {
			TransactionSupport.runAtomically(c -> runAtomatically(sqlOrders, c), connection);
		} catch (SQLException e) {
			throw new ExecutionException(e);
		}
	}
	
	/**
	 * Executes sql statements within a single transaction
	 * @param sqlOrders sql orders to be executed
	 * @param connection the {@link Connection} on which to run sql orders 
	 * @throws SQLException any error thrown by sql orders or transaction management
	 */
	public void runAtomatically(String[] sqlOrders, Connection connection) throws SQLException {
		for (String sqlOrder : sqlOrders) {
			try {
				runSqlOrder(sqlOrder, connection);
			} catch (SQLException e) {
				connection.rollback();
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
					int updatedRowCount = statement.executeUpdate(sqlOrder);
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
	
	public String[] getSqlOrders() {
		return this.sqlOrders;
	}
}
