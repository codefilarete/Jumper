package org.codefilarete.jumper.schema.metadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DerbyMetadataReaderTest extends MetadataReaderTest {
	
	@BeforeEach
	@Override
	void createDataSource() {
		dataSource = new DerbyInMemoryDataSource();
	}
	
	@Override
	protected String getDefaultCatalog() {
		return "";
	}
	
	@Override
	protected SchemaMetadataReader buildTestInstance() throws SQLException {
		return new DerbyMetadataReader(dataSource.getConnection().getMetaData());
	}
	
	/**
	 * Overridden because Derby doesn't support comments on tables
	 */
	@Test
	void giveTables() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<TableMetadata> ddlElements = testInstance.giveTables(null, null, "%OT%");
		
		// Because we use HSQLDB as a test DataSource, we get its default "PUBLIC" catalog
		// Because we use HSQLDB as a test DataSource, information is uppercased
		TableMetadata expectedMetadata = new TableMetadata("", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(expectedMetadata);
	}
	
	@Override
	void giveProcedures() throws SQLException {
	
	}
}
