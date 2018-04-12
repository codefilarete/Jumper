package org.gama.jumper;

import java.sql.SQLException;
import java.time.LocalDateTime;

import org.gama.jumper.impl.DatabaseChange;
import org.gama.lang.collection.Maps;
import org.gama.sql.DataSourceConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.DefaultResultSetReaders;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.ResultSetReader;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.junit.jupiter.api.Test;

import static org.gama.jumper.JdbcApplicationChangeStorage.DEFAULT_STORAGE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class JdbcApplicationChangeStorageTest {
	
	@Test
	public void testPersist() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(hsqldbInMemoryDataSource);
		
		HSQLDBDialect hsqldbDialect = new HSQLDBDialect();
		
		// declaring mapping of Checksum simple type
		hsqldbDialect.getColumnBinderRegistry().register(DEFAULT_STORAGE_TABLE.checksum,
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Checksum::new, Checksum::toString));
		hsqldbDialect.getJavaTypeToSqlTypeMapping().put(DEFAULT_STORAGE_TABLE.checksum, "VARCHAR(255)");
		
		// deploying table to database
		DDLSchemaGenerator ddlSchemaGenerator = new DDLSchemaGenerator(hsqldbDialect.getJavaTypeToSqlTypeMapping());
		ddlSchemaGenerator.addTables(DEFAULT_STORAGE_TABLE);
		DDLDeployer ddlDeployer = new DDLDeployer(ddlSchemaGenerator, connectionProvider);
		ddlDeployer.deployDDL();
		
		// test
		JdbcApplicationChangeStorage testInstance = new JdbcApplicationChangeStorage(connectionProvider);
		DatabaseChange databaseChange = new DatabaseChange("dummyId", false, hsqldbInMemoryDataSource, new String[] {
				"select 1 from dual"
		});
		testInstance.persist(databaseChange);
		
		// verifications
		RowIterator rowIterator = new RowIterator(
				connectionProvider.getCurrentConnection().prepareStatement("select * from " + DEFAULT_STORAGE_TABLE.getAbsoluteName()).executeQuery(),
				Maps.asMap("id", (ResultSetReader) DefaultResultSetReaders.STRING_READER)
				.add(DEFAULT_STORAGE_TABLE.createdAt.getName(), DefaultResultSetReaders.LOCALDATETIME_READER)
				.add(DEFAULT_STORAGE_TABLE.checksum.getName(), DefaultResultSetReaders.STRING_READER));
		assertTrue(rowIterator.hasNext());
		Row row = rowIterator.next();
		assertEquals(databaseChange.getIdentifier().toString(), row.get(DEFAULT_STORAGE_TABLE.id.getName()));
		assertEquals(databaseChange.computeChecksum().toString(), row.get(DEFAULT_STORAGE_TABLE.checksum.getName()));
		assertEquals(LocalDateTime.now().withNano(0), ((LocalDateTime) row.get(DEFAULT_STORAGE_TABLE.createdAt.getName())).withNano(0));
	}
	
}