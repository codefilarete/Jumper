package org.codefilarete.jumper.schema;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.testcontainers.containers.MySQLContainer;

public class MySQLDataSource extends UrlAwareDataSource {
	
	public MySQLDataSource(MySQLContainer<?> mysqlDBContainer) {
		super(mysqlDBContainer.getJdbcUrl());
        MysqlDataSource delegate = new MysqlDataSource();
        delegate.setURL(getUrl());
        delegate.setUser(mysqlDBContainer.getUsername());
        delegate.setPassword(mysqlDBContainer.getPassword());
        setDelegate(delegate);
    }
}