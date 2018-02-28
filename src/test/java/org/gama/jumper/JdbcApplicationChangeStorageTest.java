package org.gama.jumper;

import java.sql.SQLException;

import org.gama.jumper.impl.DatabaseChange;
import org.gama.lang.collection.Maps;
import org.gama.sql.DataSourceConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		hsqldbDialect.getColumnBinderRegistry().register(JdbcApplicationChangeStorage.checksum, new LambdaParameterBinder<>(
				(resultSet, columnName) -> new Checksum(resultSet.getString(columnName)),
				(preparedStatement, valueIndex, value) -> preparedStatement.setString(valueIndex, value.toString())));
		hsqldbDialect.getJavaTypeToSqlTypeMapping().put(JdbcApplicationChangeStorage.checksum, "VARCHAR(255)");
		
		// deploying table to database
		DDLSchemaGenerator ddlSchemaGenerator = new DDLSchemaGenerator(hsqldbDialect.getJavaTypeToSqlTypeMapping());
		ddlSchemaGenerator.addTables(JdbcApplicationChangeStorage.TABLE_STORAGE);
		DDLDeployer ddlDeployer = new DDLDeployer(ddlSchemaGenerator, connectionProvider);
		ddlDeployer.deployDDL();
		
		// test
		JdbcApplicationChangeStorage testInstance = new JdbcApplicationChangeStorage(connectionProvider);
		testInstance.persist(new DatabaseChange("dummyId", false, hsqldbInMemoryDataSource, new String[] {
				"select 1 from dual"
		}));
		
		// verifications
		RowIterator rowIterator = new RowIterator(
				connectionProvider.getCurrentConnection().prepareStatement("select * from " + JdbcApplicationChangeStorage.TABLE_STORAGE
						.getAbsoluteName()).executeQuery(),
				Maps.asMap("id", DefaultParameterBinders.STRING_BINDER)
		);
		assertTrue(rowIterator.hasNext());
		Row row = rowIterator.next();
		assertEquals("dummyId", row.get("id"));
	}
	
}