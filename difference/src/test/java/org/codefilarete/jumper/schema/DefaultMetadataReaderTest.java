package org.codefilarete.jumper.schema;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Set;

import org.codefilarete.jumper.schema.MetadataReader.TypeInfo;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

class DefaultMetadataReaderTest {
	
	@Test
	void giveTables() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<TableMetadata> ddlElements = testInstance.giveTables("integrationTests", null, "%");
		ddlElements.forEach(t -> {
			System.out.println(t.getCatalog() + "." + t.getSchema() + "." + t.getName());
		});
	}
	
	@Test
	void giveExportedKeys() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveExportedKeys("integrationTests", null, "%");
		ddlElements.stream().sorted(Comparator.comparing(ForeignKeyMetadata::getName)).forEach(t -> {
			System.out.println(t);
		});
	}
	
	@Test
	void giveImportedKeys() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ForeignKeyMetadata> ddlElements = testInstance.giveImportedKeys("integrationTests", null, "Prescription");
		ddlElements.stream().sorted(Comparator.comparing(ForeignKeyMetadata::getName)).forEach(t -> {
			System.out.println(t);
		});
	}
	
	@Test
	void givePrimaryKeys() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		PrimaryKeyMetadata ddlElement = testInstance.givePrimaryKey("integrationTests", null, "Prescription");
		System.out.println(ddlElement);
	}
	
	@Test
	void giveColumns() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ColumnMetadata> ddlElements = testInstance.giveColumns("integrationTests", null, "Patient");
		ddlElements.forEach(t -> {
			System.out.println(t);
//			System.out.println(t.getName() + ": " + t.getSqlType() + Nullable.nullable(t.getSize()).map(s -> "(" + s + ")").getOr(""));
		});
	}
	
	@Test
	void giveColumnTypes() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<TypeInfo> ddlElements = testInstance.giveColumnTypes();
		ddlElements.forEach(t -> {
			System.out.println(t);
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
	
	@Test
	void giveProcedures() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		DefaultMetadataReader testInstance = new DefaultMetadataReader(dataSource.getConnection().getMetaData());
		Set<ProcedureMetadata> ddlElements = testInstance.giveProcedures(null, null, "%");
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
}