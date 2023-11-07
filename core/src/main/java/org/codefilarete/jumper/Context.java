package org.codefilarete.jumper;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * Class aimed at storing information about context of execution.
 * 
 * Can be a combination of database information (vendor, url, ...), environment (OS, server, ...), Context and Dependency Injection, etc.
 * All of this is implementation specific, default gives {@link DatabaseSignet}.
 * 
 * @author Guillaume Mary
 */
public class Context {
	
	private final DatabaseSignet databaseSignet;
	
	public Context(DatabaseSignet databaseSignet) {
		this.databaseSignet = databaseSignet;
	}
	
	public DatabaseSignet getDatabaseSignet() {
		return databaseSignet;
	}
}
