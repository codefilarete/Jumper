package org.codefilarete.jumper.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.codefilarete.jumper.Context;
import org.codefilarete.jumper.ddl.dsl.DDLEase;
import org.codefilarete.jumper.ddl.engine.Dialect;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * @author Guillaume Mary
 */
class DDLChangeTest {
	
	@Test
	void run() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection connectionMock = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(connectionMock);
		
		Statement statementMock = mock(Statement.class);
		when(connectionMock.createStatement()).thenReturn(statementMock);
		
		DDLChange testInstance = new DDLChange("dummyChangeId", DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.notNull()
						.autoIncrement()
				.defaultValue("'hello world !'")
				.addColumn("col2", "bigint")
				.build());
		testInstance.run(new Context(new Dialect(), dataSourceMock.getConnection()));
		
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		verify(connectionMock, times(1)).createStatement();
		verify(statementMock, times(1)).execute(sqlCaptor.capture());
		assertEquals("create table toto(col1 varchar(100) not null auto_increment default 'hello world !', col2 bigint)", sqlCaptor.getValue());
	}
}