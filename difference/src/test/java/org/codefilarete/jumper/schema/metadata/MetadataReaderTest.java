package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.tool.Duo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

abstract class MetadataReaderTest {
	
	public static final String METADATA_READER_TEST_SCHEMA = "MetadataReaderTest_Schema";
	protected DataSource dataSource;
	
	@BeforeEach
	abstract void createDataSource();
	
	abstract String getDefaultCatalog();
	
	abstract protected SchemaMetadataReader buildTestInstance() throws SQLException;
	
	@Test
	void giveTables() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		connection.createStatement().execute("comment on table " + METADATA_READER_TEST_SCHEMA + ".Toto is 'Hello world'");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<TableMetadata> ddlElements = testInstance.giveTables(null, null, "%OT%");
		
		TableMetadata tableMetadata = new TableMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		tableMetadata.setRemarks("Hello world");
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(tableMetadata);
	}
	
	@Test
	void giveExportedKeys() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Parent(id int primary key)");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Child(id int primary key, parentId int)");
		connection.createStatement().execute("alter table " + METADATA_READER_TEST_SCHEMA + ".Child add constraint FK_Child_Parent foreign key (parentId) references " + METADATA_READER_TEST_SCHEMA + ".Parent(id)");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveExportedKeys(null, METADATA_READER_TEST_SCHEMA.toUpperCase(), "%AREN%");
		
		ForeignKeyMetadata expectedMetadata = new ForeignKeyMetadata("FK_CHILD_PARENT", getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "CHILD", getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "PARENT");
		expectedMetadata.addColumn("PARENTID", "ID");
		
		assertThat(ddlElements)
				.usingRecursiveFieldByFieldElementComparator()
				.containsExactlyInAnyOrder(expectedMetadata);
	}
	
	@Test
	void giveImportedKeys() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Parent(id int primary key)");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Child(id int primary key, parentId int)");
		connection.createStatement().execute("alter table " + METADATA_READER_TEST_SCHEMA + ".Child add constraint FK_Child_Parent foreign key (parentId) references " + METADATA_READER_TEST_SCHEMA + ".Parent(id)");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveImportedKeys(null, METADATA_READER_TEST_SCHEMA.toUpperCase(), "%HIL%");
		
		ForeignKeyMetadata expectedMetadata = new ForeignKeyMetadata("FK_CHILD_PARENT", getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "CHILD", getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "PARENT");
		expectedMetadata.addColumn("PARENTID", "ID");
		
		assertThat(ddlElements)
				.usingRecursiveFieldByFieldElementComparator()
				.containsExactlyInAnyOrder(expectedMetadata);
	}
	
	@Test
	void givePrimaryKeys() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int primary key, name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<PrimaryKeyMetadata> ddlElements = testInstance.givePrimaryKeys(null, METADATA_READER_TEST_SCHEMA.toUpperCase(), "%OT%");
		
		PrimaryKeyMetadata expectedMetadata = new PrimaryKeyMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO", "ignored at comparison");
		expectedMetadata.addColumn("ID");
		
		assertThat(ddlElements)
				// we ignore PK name because their format is too varying depending on vendor (some of them depend on clock)
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("name")
				.containsExactlyInAnyOrder(expectedMetadata);
	}
	
	@Test
	void giveColumns() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id decimal(10,2), name varchar(200))");
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Titi(id int)");
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<ColumnMetadata> ddlElements = testInstance.giveColumns(null, null, "%OT%");
		
		ColumnMetadata columnMetadata1 = new ColumnMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		columnMetadata1.setName("ID");
		columnMetadata1.setNullable(true);
		columnMetadata1.setSqlType(JDBCType.DECIMAL);
		columnMetadata1.setSize(10);
		columnMetadata1.setPrecision(2);
		columnMetadata1.setPosition(1);
		ColumnMetadata columnMetadata2 = new ColumnMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		columnMetadata2.setName("NAME");
		columnMetadata2.setNullable(true);
		columnMetadata2.setSqlType(JDBCType.VARCHAR);
		columnMetadata2.setSize(200);
		columnMetadata2.setPrecision(null);
		columnMetadata2.setPosition(2);
		
		assertThat(ddlElements)
				// we ignore vendorType because it depends too much on vendors and is not important info
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("vendorType")
				.containsExactlyInAnyOrder(columnMetadata1, columnMetadata2);
	}
	
	/**
	 * Made abstract because procedures contain vendor-specific SQL
	 */
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
		
		SchemaMetadataReader testInstance = buildTestInstance();
		Set<IndexMetadata> ddlElements = testInstance.giveIndexes(null, null, "%OT%", null);
		
		IndexMetadata indexMetadata1 = new IndexMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		indexMetadata1.setName("MYINDEX");
		indexMetadata1.setUnique(true);
		indexMetadata1.getColumns().add(new Duo<>("FIRSTNAME", true));
		
		IndexMetadata indexMetadata2 = new IndexMetadata(getDefaultCatalog(), METADATA_READER_TEST_SCHEMA.toUpperCase(), "TOTO");
		indexMetadata2.setName("MYINDEX2");
		indexMetadata2.setUnique(false);
		indexMetadata2.getColumns().add(new Duo<>("LASTNAME", true));
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(indexMetadata1, indexMetadata2);
	}
}