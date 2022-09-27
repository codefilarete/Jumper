package org.codefilarete.jumper.schema;

import java.sql.SQLException;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;

public class MariaDBDataSource extends UrlAwareDataSource {

    public MariaDBDataSource(MariaDBContainer<?> mariaDBContainer) {
        super(mariaDBContainer.getJdbcUrl());
        try {
            MariaDbDataSource delegate = new MariaDbDataSource(getUrl());
            delegate.setUser(mariaDBContainer.getUsername());
            delegate.setPassword(mariaDBContainer.getPassword());
            setDelegate(delegate);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}