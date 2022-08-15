package org.codefilarete.jumper;

import java.sql.SQLException;
import java.time.LocalDateTime;

import org.codefilarete.jumper.ApplicationChangeStorage.ChangeSignet;
import org.codefilarete.jumper.impl.SQLChange;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.jumper.JdbcApplicationChangeStorage.DEFAULT_STORAGE_TABLE;

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
		SQLChange sqlChange = new SQLChange("dummyId", "select 1 from dual");
		Checksum checksum = new Checksum("a robust fake checksum");
		testInstance.persist(new ChangeSignet(sqlChange.getIdentifier(), checksum));
		
		// verifications
		RowIterator rowIterator = new RowIterator(
				connectionProvider.giveConnection().prepareStatement("select * from " + DEFAULT_STORAGE_TABLE.getAbsoluteName()).executeQuery(),
				Maps.asMap("id", (ResultSetReader) DefaultResultSetReaders.STRING_READER)
				.add(DEFAULT_STORAGE_TABLE.createdAt.getName(), DefaultResultSetReaders.LOCALDATETIME_READER)
				.add(DEFAULT_STORAGE_TABLE.checksum.getName(), DefaultResultSetReaders.STRING_READER));
		assertThat(rowIterator.hasNext()).isTrue();
		Row row = rowIterator.next();
		assertThat(row.get(DEFAULT_STORAGE_TABLE.id.getName())).isEqualTo(sqlChange.getIdentifier().toString());
		assertThat(row.get(DEFAULT_STORAGE_TABLE.checksum.getName())).isEqualTo(checksum.toString());
		assertThat(((LocalDateTime) row.get(DEFAULT_STORAGE_TABLE.createdAt.getName())).withNano(0)).isEqualTo(LocalDateTime.now().withNano(0));
	}
	
}