package org.codefilarete.jumper;

import java.sql.Connection;

import org.codefilarete.stalactite.sql.ConnectionProvider;

public interface SeparateConnectionProvider extends ConnectionProvider {
	
	Connection giveSeparateConnection();
}
