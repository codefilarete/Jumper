package org.gama.jumper.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

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
	public void testRun() throws ExecutionException, SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection connectionMock = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(connectionMock);
		
		Statement statementMock = mock(Statement.class);
		when(connectionMock.createStatement()).thenReturn(statementMock);
		
		DatabaseChange testInstance = new DatabaseChange("dummyChangeId", true, dataSourceMock, new String[] {
				"insert into X(a, b, c) values (1, 2, 3)",
				"update X set a = 4",
				"delete X where a = 1",
				"select * from X",
				"create table X()"
		});
		testInstance.run();
		
		verify(connectionMock, times(5)).createStatement();
		verify(statementMock, times(3)).executeUpdate(anyString());
		verify(statementMock, times(1)).executeQuery(anyString());
		verify(statementMock, times(1)).execute(anyString());
	}
	
}