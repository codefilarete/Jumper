package org.codefilarete.jumper.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.codefilarete.jumper.Context;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * @author Guillaume Mary
 */
public class DatabaseChangeTest {
	
	@Test
	void run() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection connectionMock = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(connectionMock);
		
		Statement statementMock = mock(Statement.class);
		when(connectionMock.createStatement()).thenReturn(statementMock);
		
		SQLChange testInstance = new SQLChange("dummyChangeId", true, new String[] {
				"insert into X(a, b, c) values (1, 2, 3)",
				"update X set a = 4",
				"delete X where a = 1",
				"select * from X",
				"create table X()"
		});
		testInstance.run(new Context(null, dataSourceMock.getConnection()));
		
		verify(connectionMock, times(5)).createStatement();
		verify(statementMock, times(3)).executeLargeUpdate(anyString());
		verify(statementMock, times(1)).executeQuery(anyString());
		verify(statementMock, times(1)).execute(anyString());
	}
	
}