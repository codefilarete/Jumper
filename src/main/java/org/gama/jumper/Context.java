package org.gama.jumper;

import java.sql.Connection;

import org.gama.jumper.ddl.engine.Dialect;

/**
 * Class aimed at storing information about context of execution.
 * 
 * Can be a combination of database information (vendor, url, ...), environment (OS, server, ...), Context and Dependency Injection, etc.
 * All of this is implementation specific, default gives nothing.
 * 
 * @author Guillaume Mary
 */
public class Context {
	
	private final Dialect dialect;
	private final Connection connection;
	
	public Context(Dialect dialect, Connection connection) {
		this.dialect = dialect;
		this.connection = connection;
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	public Connection getConnection() {
		return connection;
	}
}
