package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.stalactite.sql.ConnectionProvider;

public interface FluentChangeLog {
	
	void applyTo(ConnectionProvider connectionProvider);
}
