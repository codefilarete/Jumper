package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultMetadataReaderTest {
	
	public static final String METADATA_READER_TEST_SCHEMA = "MetadataReaderTest_Schema";
	private static DataSource dataSource;
	
	@BeforeEach
	void createStructures() throws SQLException {
		dataSource = new HSQLDBInMemoryDataSource();
		
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, name varchar(200))");
		connection.createStatement().execute("create unique index MyIndex ON " + METADATA_READER_TEST_SCHEMA + ".Toto(name)");
	}
	
	@Test
	void giveTables() throws SQLException {
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<TableMetadata> ddlElements = testInstance.giveTables("integrationTests", null, "%");
		ddlElements.forEach(t -> {
			System.out.println(t.getCatalog() + "." + t.getSchema() + "." + t.getName());
		});
	}
	
	@Test
	void giveExportedKeys() throws SQLException {
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveExportedKeys("integrationTests", null, "%");
		ddlElements.stream().sorted(Comparator.comparing(ForeignKeyMetadata::getName)).forEach(t -> {
			System.out.println(t);
		});
	}
	
	@Test
	void giveImportedKeys() throws SQLException {
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveImportedKeys("integrationTests", null, "Prescription");
		ddlElements.stream().sorted(Comparator.comparing(ForeignKeyMetadata::getName)).forEach(t -> {
			System.out.println(t);
		});
	}
	
	@Test
	void givePrimaryKeys() throws SQLException {
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		PrimaryKeyMetadata ddlElement = testInstance.givePrimaryKey("integrationTests", null, "Prescription");
		System.out.println(ddlElement);
	}
	
	@Test
	void giveColumns() throws SQLException {
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ColumnMetadata> ddlElements = testInstance.giveColumns("integrationTests", null, "Patient");
		ddlElements.forEach(t -> {
			System.out.println(t);
//			System.out.println(t.getName() + ": " + t.getSqlType() + Nullable.nullable(t.getSize()).map(s -> "(" + s + ")").getOr(""));
		});
	}
	
	@Test
	void giveProcedures() throws SQLException {
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ProcedureMetadata> ddlElements = testInstance.giveProcedures(null, null, "%");
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
	
	@Test
	void giveIndexes() throws SQLException {
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<IndexMetadata> ddlElements = testInstance.giveIndexes(null, null, "TOTO");
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
}