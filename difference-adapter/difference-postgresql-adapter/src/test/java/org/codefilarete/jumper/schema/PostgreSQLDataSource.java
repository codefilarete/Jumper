package org.codefilarete.jumper.schema;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgreSQLDataSource extends UrlAwareDataSource {
	
	public PostgreSQLDataSource(PostgreSQLContainer<?> mariaDBContainer) {
		super(mariaDBContainer.getJdbcUrl());
		PGSimpleDataSource delegate = new PGSimpleDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser(mariaDBContainer.getUsername());
		delegate.setPassword(mariaDBContainer.getPassword());
		setDelegate(delegate);
	}
}