package org.codefilarete.jumper;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;

import org.codefilarete.jumper.ChangeStorage.ChangeSignet;
import org.codefilarete.jumper.impl.JdbcChangeStorage;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
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
import static org.codefilarete.jumper.impl.JdbcChangeStorage.DEFAULT_STORAGE_TABLE;

/**
 * @author Guillaume Mary
 */
public class JdbcChangeStorageTest {
	
	@Test
	void persist() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		SimpleConnectionProvider connectionProvider = new SimpleConnectionProvider(hsqldbInMemoryDataSource.getConnection());
		
		Dialect hsqldbDialect = new HSQLDBDialectBuilder().build();
		
		// declaring mapping of Checksum simple type
		hsqldbDialect.getColumnBinderRegistry().register(DEFAULT_STORAGE_TABLE.checksum,
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Checksum::new, Checksum::toString));
		hsqldbDialect.getSqlTypeRegistry().put(DEFAULT_STORAGE_TABLE.checksum, "VARCHAR(255)");
		
		// deploying table to database
		DDLDeployer ddlDeployer = new DDLDeployer(hsqldbDialect, connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(DEFAULT_STORAGE_TABLE);
		ddlDeployer.deployDDL();
		
		// test
		JdbcChangeStorage testInstance = new JdbcChangeStorage(connectionProvider);
		Checksum checksum = new Checksum("a robust fake checksum");
		testInstance.persist(new ChangeSignet("dummyId", checksum));
		
		// verifications
		Map<String, ResultSetReader<?>> readers = Maps.forHashMap(String.class, (Class<ResultSetReader<?>>) (Class) ResultSetReader.class)
				.add("id", DefaultResultSetReaders.STRING_READER)
				.add(DEFAULT_STORAGE_TABLE.createdAt.getName(), DefaultResultSetReaders.LOCALDATETIME_READER)
				.add(DEFAULT_STORAGE_TABLE.checksum.getName(), DefaultResultSetReaders.STRING_READER);
		RowIterator rowIterator = new RowIterator(
				connectionProvider.giveConnection().prepareStatement("select * from " + DEFAULT_STORAGE_TABLE.getAbsoluteName()).executeQuery(),
				readers);
		assertThat(rowIterator.hasNext()).isTrue();
		Row row = rowIterator.next();
		assertThat(row.get(DEFAULT_STORAGE_TABLE.id.getName())).isEqualTo("dummyId");
		assertThat(row.get(DEFAULT_STORAGE_TABLE.checksum.getName())).isEqualTo(checksum.toString());
		assertThat(((LocalDateTime) row.get(DEFAULT_STORAGE_TABLE.createdAt.getName())).withNano(0)).isEqualTo(LocalDateTime.now().withNano(0));
	}
	
}