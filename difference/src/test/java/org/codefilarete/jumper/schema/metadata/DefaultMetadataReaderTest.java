package org.codefilarete.jumper.schema.metadata;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Duo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

abstract class DefaultMetadataReaderTest {
	
	public static final String METADATA_READER_TEST_SCHEMA = "MetadataReaderTest_Schema";
	protected static HSQLDBInMemoryDataSource dataSource;
	
	@BeforeEach
	void createStructures() {
		dataSource = new HSQLDBInMemoryDataSource();
	}
	
	protected DefaultMetadataReader buildTestInstance() throws SQLException {
		return new DefaultMetadataReader(dataSource.getConnection().getMetaData());
	}
	
	@Test
	void giveTables() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		connection.createStatement().execute("comment on table " + METADATA_READER_TEST_SCHEMA + ".Toto is 'Hello world'");
		
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<TableMetadata> ddlElements = testInstance.giveTables(null, null, "%OT%");
		
		// Because we use HSQLDB as a test DataSource, we get its default "PUBLIC" catalog
		// Because we use HSQLDB as a test DataSource, information is uppercased
		TableMetadata tableMetadata = new TableMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		tableMetadata.setRemarks("Hello world");
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(tableMetadata);
	}
	
	@Test
	void giveExportedKeys() throws SQLException {
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveExportedKeys("integrationTests", null, "%");
		ddlElements.stream().sorted(Comparator.comparing(ForeignKeyMetadata::getName)).forEach(t -> {
			System.out.println(t);
		});
	}
	
	@Test
	void giveImportedKeys() throws SQLException {
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveImportedKeys("integrationTests", null, "Prescription");
		ddlElements.stream().sorted(Comparator.comparing(ForeignKeyMetadata::getName)).forEach(t -> {
			System.out.println(t);
		});
	}
	
	@Test
	void givePrimaryKeys() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int primary key, name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<PrimaryKeyMetadata> ddlElements = testInstance.givePrimaryKeys(null, METADATA_READER_TEST_SCHEMA.toUpperCase(), "%OT%");
		
		// HSQLDB default PK name format, or check if your metadata reader fills it
		PrimaryKeyMetadata expectedMetadata = new PrimaryKeyMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO", "SYS_PK_10091");
		expectedMetadata.addColumn("ID");
		
		assertThat(ddlElements)
				.usingRecursiveFieldByFieldElementComparator()
				.containsExactlyInAnyOrder(expectedMetadata);
	}
	
	@Test
	void giveColumns() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<ColumnMetadata> ddlElements = testInstance.giveColumns(null, null, "%OT%");
		
		// Because we use HSQLDB as a test DataSource, we get its default "PUBLIC" catalog
		// Because we use HSQLDB as a test DataSource, information is uppercased
		ColumnMetadata columnMetadata1 = new ColumnMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		columnMetadata1.setName("ID");
		columnMetadata1.setNullable(true);
		columnMetadata1.setSqlType(JDBCType.INTEGER);
		columnMetadata1.setVendorType("INTEGER");
		columnMetadata1.setSize(32);
		columnMetadata1.setPrecision(0);
		columnMetadata1.setPosition(1);
		ColumnMetadata columnMetadata2 = new ColumnMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		columnMetadata2.setName("NAME");
		columnMetadata2.setNullable(true);
		columnMetadata2.setSqlType(JDBCType.VARCHAR);
		columnMetadata2.setVendorType("VARCHAR");
		columnMetadata2.setSize(200);
		columnMetadata2.setPrecision(null);
		columnMetadata2.setPosition(2);
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(columnMetadata1, columnMetadata2);
	}
	
	@Test
	abstract void giveProcedures() throws SQLException;
	
	@Test
	void giveIndexes() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, firstname varchar(200), lastname varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		connection.createStatement().execute("create unique index MyIndex ON " + METADATA_READER_TEST_SCHEMA + ".Toto(firstname)");
		connection.createStatement().execute("create index MyIndex2 ON " + METADATA_READER_TEST_SCHEMA + ".Toto(lastname)");
		
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<IndexMetadata> ddlElements = testInstance.giveIndexes(null, null, "%OT%", null);
		
		IndexMetadata indexMetadata1 = new IndexMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		indexMetadata1.setName("MYINDEX");
		indexMetadata1.setUnique(true);
		indexMetadata1.getColumns().add(new Duo<>("FIRSTNAME", true));
		
		IndexMetadata indexMetadata2 = new IndexMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		indexMetadata2.setName("MYINDEX2");
		indexMetadata2.setUnique(false);
		indexMetadata2.getColumns().add(new Duo<>("LASTNAME", true));
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(indexMetadata1, indexMetadata2);
	}
}