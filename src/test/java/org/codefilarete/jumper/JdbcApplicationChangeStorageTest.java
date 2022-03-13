package org.codefilarete.jumper;

import java.sql.SQLException;
import java.time.LocalDateTime;

import org.codefilarete.jumper.impl.SQLChange;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.persistence.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.junit.jupiter.api.Test;

import static org.codefilarete.jumper.JdbcApplicationChangeStorage.DEFAULT_STORAGE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class JdbcApplicationChangeStorageTest {
	
	@Test
	public void testPersist() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		CurrentThreadConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(hsqldbInMemoryDataSource);
		
		HSQLDBDialect hsqldbDialect = new HSQLDBDialect();
		
		// declaring mapping of Checksum simple type
		hsqldbDialect.getColumnBinderRegistry().register(DEFAULT_STORAGE_TABLE.checksum,
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Checksum::new, Checksum::toString));
		hsqldbDialect.getSqlTypeRegistry().put(DEFAULT_STORAGE_TABLE.checksum, "VARCHAR(255)");
		
		// deploying table to database
		DDLDeployer ddlDeployer = new DDLDeployer(hsqldbDialect.getSqlTypeRegistry(), connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(DEFAULT_STORAGE_TABLE);
		ddlDeployer.deployDDL();
		
		// test
		JdbcApplicationChangeStorage testInstance = new JdbcApplicationChangeStorage(connectionProvider);
		SQLChange SQLChange = new SQLChange("dummyId", false, new String[] {
				"select 1 from dual"
		});
		testInstance.persist(SQLChange);
		
		// verifications
		RowIterator rowIterator = new RowIterator(
				connectionProvider.giveConnection().prepareStatement("select * from " + DEFAULT_STORAGE_TABLE.getAbsoluteName()).executeQuery(),
				Maps.asMap("id", (ResultSetReader) DefaultResultSetReaders.STRING_READER)
				.add(DEFAULT_STORAGE_TABLE.createdAt.getName(), DefaultResultSetReaders.LOCALDATETIME_READER)
				.add(DEFAULT_STORAGE_TABLE.checksum.getName(), DefaultResultSetReaders.STRING_READER));
		assertTrue(rowIterator.hasNext());
		Row row = rowIterator.next();
		assertEquals(SQLChange.getIdentifier().toString(), row.get(DEFAULT_STORAGE_TABLE.id.getName()));
		assertEquals(SQLChange.computeChecksum().toString(), row.get(DEFAULT_STORAGE_TABLE.checksum.getName()));
		assertEquals(LocalDateTime.now().withNano(0), ((LocalDateTime) row.get(DEFAULT_STORAGE_TABLE.createdAt.getName())).withNano(0));
	}
	
}