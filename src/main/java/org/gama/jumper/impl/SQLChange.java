package org.gama.jumper.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.gama.jumper.AbstractChange;
import org.gama.jumper.Checksum;
import org.gama.jumper.Context;
import org.gama.jumper.ExecutionException;
import org.gama.jumper.ChangeId;
import org.gama.lang.sql.TransactionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An update dedicated to SQL execution
 * 
 * @author Guillaume Mary
 */
public class SQLChange extends AbstractChange {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SQLChange.class);
	
	private final String[] sqlOrders;
	
	public SQLChange(ChangeId changeId, boolean shouldAlwaysRun, String[] sqlOrders) {
		super(changeId, shouldAlwaysRun);
		this.sqlOrders = sqlOrders;
	}
	
	public SQLChange(String identifier, boolean shouldAlwaysRun, String[] sqlOrders) {
		this(new ChangeId(identifier), shouldAlwaysRun, sqlOrders);
	}
	
	/**
	 * Implemented to compute the Checksum from SQL orders.
	 * 
	 * @return a {@link Checksum} of SQL orders
	 */
	@Override
	public Checksum computeChecksum() {
		StringBuilder allSQL = new StringBuilder(200);
		for (String sqlOrder : sqlOrders) {
			allSQL.append(sqlOrder);
		}
		return StringChecksumer.INSTANCE.checksum(allSQL.toString());
	}
	
	@Override
	public void run(Context context) throws ExecutionException {
		try {
			TransactionSupport.runAtomically(c -> runAtomatically(sqlOrders, c), context.getConnection());
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
