package org.codefilarete.jumper.schema.metadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.ProcedureMetadata.ProcedureType;
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
	
	@Test
	@Override
	void giveProcedures() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		
		connection.createStatement().execute("CREATE PROCEDURE " + METADATA_READER_TEST_SCHEMA + ".DOSOMETHING()" +
				" PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'java.lang.System.gc'");
		connection.createStatement().execute("CREATE FUNCTION " + METADATA_READER_TEST_SCHEMA + ".DOSOMETHING2()" +
				" RETURNS DOUBLE PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'java.lang.Math.random'");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<ProcedureMetadata> ddlElements = testInstance.giveProcedures(null, METADATA_READER_TEST_SCHEMA.toUpperCase(), "%");
		
		ProcedureMetadata expectedMetadata1 = new ProcedureMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "DOSOMETHING");
		expectedMetadata1.setType(ProcedureType.PROCEDURE);
		expectedMetadata1.setRemarks("java.lang.System.gc");
		ProcedureMetadata expectedMetadata2 = new ProcedureMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "DOSOMETHING2");
		expectedMetadata2.setType(ProcedureType.FUNCTION);
		expectedMetadata2.setRemarks("java.lang.Math.random");
		
		assertThat(ddlElements)
				// we ignore specificName because it depends too random
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("specificName")
				.containsExactlyInAnyOrder(expectedMetadata1, expectedMetadata2);
	}
}
