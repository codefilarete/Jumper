package org.codefilarete.jumper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.codefilarete.jumper.impl.JdbcUpdateProcessSemaphore;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.byLessThan;
import static org.codefilarete.jumper.impl.JdbcUpdateProcessSemaphore.DEFAULT_STORAGE_TABLE;

class JdbcUpdateProcessSemaphoreTest {

	@Test
	void insertRow() throws SQLException, UnknownHostException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		SeparateConnectionProvider connectionProvider = new DataSourceConnectionProvider(hsqldbInMemoryDataSource);
		
		Dialect hsqldbDialect = new HSQLDBDialectBuilder().build();
		
		// deploying table to database
		DDLDeployer ddlDeployer = new DDLDeployer(hsqldbDialect, connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(JdbcUpdateProcessSemaphore.DEFAULT_STORAGE_TABLE);
		ddlDeployer.deployDDL();
		
		JdbcUpdateProcessSemaphore testInstance = new JdbcUpdateProcessSemaphore(connectionProvider);
		
		// test
		testInstance.acquireLock("dummy identifier");
		
		// verifications
		Map<String, ResultSetReader<?>> readers = Maps.forHashMap(String.class, (Class<ResultSetReader<?>>) (Class) ResultSetReader.class)
				.add("id", DefaultResultSetReaders.STRING_READER)
				.add(DEFAULT_STORAGE_TABLE.createdAt.getName(), new LambdaParameterBinder<>(DefaultParameterBinders.LONG_BINDER, Instant::ofEpochMilli, Instant::toEpochMilli))
				.add(DEFAULT_STORAGE_TABLE.createdBy.getName(), DefaultResultSetReaders.STRING_READER);
		RowIterator rowIterator = new RowIterator(
				connectionProvider.giveConnection().prepareStatement("select * from " + DEFAULT_STORAGE_TABLE.getAbsoluteName()).executeQuery(),
				readers);
		assertThat(rowIterator.hasNext()).isTrue();
		Row row = rowIterator.next();
		assertThat(row.get(DEFAULT_STORAGE_TABLE.id.getName())).isEqualTo("dummy identifier");
		assertThat(((Instant) row.get(DEFAULT_STORAGE_TABLE.createdAt.getName()))).isCloseTo(Instant.now(), byLessThan(1, ChronoUnit.SECONDS));
		assertThat((String) row.get(DEFAULT_STORAGE_TABLE.createdBy.getName())).contains(InetAddress.getLocalHost().getHostName());
	}
	
	@Test
	void insertRow_calledTwice_throwsException() {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		SeparateConnectionProvider connectionProvider = new DataSourceConnectionProvider(hsqldbInMemoryDataSource);
		
		Dialect hsqldbDialect = new HSQLDBDialectBuilder().build();
		
		// deploying table to database
		DDLDeployer ddlDeployer = new DDLDeployer(hsqldbDialect, connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(JdbcUpdateProcessSemaphore.DEFAULT_STORAGE_TABLE);
		ddlDeployer.deployDDL();
		
		JdbcUpdateProcessSemaphore testInstance = new JdbcUpdateProcessSemaphore(connectionProvider);
		
		// test
		testInstance.acquireLock("dummy identifier");
		assertThatCode(() -> testInstance.acquireLock("dummy identifier"))
				.hasMessageMatching("Can't obtain lock to process changes : a lock is already acquired by .+ since .+");
	}
	
	@Test
	void deleteRow() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		SeparateConnectionProvider connectionProvider = new DataSourceConnectionProvider(hsqldbInMemoryDataSource);
		
		Dialect hsqldbDialect = new HSQLDBDialectBuilder().build();
		
		// deploying table to database
		DDLDeployer ddlDeployer = new DDLDeployer(hsqldbDialect, connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(JdbcUpdateProcessSemaphore.DEFAULT_STORAGE_TABLE);
		ddlDeployer.deployDDL();
		
		JdbcUpdateProcessSemaphore testInstance = new JdbcUpdateProcessSemaphore(connectionProvider);
		
		// test
		testInstance.acquireLock("dummy identifier");
		
		testInstance.releaseLock("dummy identifier");
		
		// verifications
		Map<String, ResultSetReader<?>> readers = Maps.forHashMap(String.class, (Class<ResultSetReader<?>>) (Class) ResultSetReader.class)
				.add("id", DefaultResultSetReaders.STRING_READER)
				.add(DEFAULT_STORAGE_TABLE.createdAt.getName(), new LambdaParameterBinder<>(DefaultParameterBinders.LONG_BINDER, Instant::ofEpochMilli, Instant::toEpochMilli))
				.add(DEFAULT_STORAGE_TABLE.createdBy.getName(), DefaultResultSetReaders.STRING_READER);
		RowIterator rowIterator = new RowIterator(
				connectionProvider.giveConnection().prepareStatement("select * from " + DEFAULT_STORAGE_TABLE.getAbsoluteName()).executeQuery(),
				readers);
		assertThat(rowIterator.hasNext()).withFailMessage(() -> "Expecting no result but found " + rowIterator.next().getContent()).isFalse();
	}
}