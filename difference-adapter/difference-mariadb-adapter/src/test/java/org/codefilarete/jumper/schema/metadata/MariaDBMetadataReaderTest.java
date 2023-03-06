package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.Set;

import org.codefilarete.jumper.schema.MariaDBDataSource;
import org.codefilarete.jumper.schema.MariaDBTest;
import org.codefilarete.tool.io.IOs;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MariaDBMetadataReaderTest {
	
	@Nested
	class IntegrationTest extends MariaDBTest {
		
		@Test
		void giveSequences() throws SQLException {
			
			DataSource dataSource = new MariaDBDataSource(mariadb);
			
			MariaDBMetadataReader testInstance = new MariaDBMetadataReader(dataSource.getConnection().getMetaData());
			Set<SequenceMetadata> ddlElements = testInstance.giveSequences(null, null);
			ddlElements.forEach(t -> {
				System.out.println(t.getName());
			});
		}
		
		@Test
		void giveExportedKeys() throws SQLException {
			
			DataSource dataSource = new MariaDBDataSource(mariadb);
			
			Connection connection = dataSource.getConnection();
			String schema1Name = "giveExportedKeys_schema";
			connection.createStatement().execute("create schema " + schema1Name);
			connection.createStatement().execute("use " + schema1Name);
			connection.createStatement().execute("create table " + schema1Name + ".Toto(tata_id int)");
			connection.createStatement().execute("create table " + schema1Name + ".Tata(id int)");
			// With MariaDB foreign key must references unique-indexed columns (else a cryptic error is thrown)
			connection.createStatement().execute("alter table Toto add unique index (tata_id)");
			connection.createStatement().execute("alter table Tata add unique index (id)");
			connection.createStatement().execute("alter table Toto add constraint aa foreign key (tata_id) references Tata (id)");
			
			String schema2Name = "giveExportedKeys_schema2";
			connection.createStatement().execute("create schema " + schema2Name);
			connection.createStatement().execute("use " + schema2Name);
			connection.createStatement().execute("create table " + schema2Name + ".Tutu(titi_id int)");
			connection.createStatement().execute("create table " + schema2Name + ".Titi(id int)");
			// With MariaDB foreign key must references unique-indexed columns (else a cryptic error is thrown)
			connection.createStatement().execute("alter table Tutu add unique index (titi_id)");
			connection.createStatement().execute("alter table Titi add unique index (id)");
			connection.createStatement().execute("alter table Tutu add constraint bb foreign key (titi_id) references Titi (id)");
			
			MariaDBMetadataReader testInstance = new MariaDBMetadataReader(connection.getMetaData());
			Set<ForeignKeyMetadata> ddlElements = testInstance.giveExportedKeys(null, schema1Name, "%");
			assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactly(
					new ForeignKeyMetadata("aa", null, schema1Name, "Toto", null, schema1Name, "Tata")
							.addColumn("tata_id", "id"));
			
			ddlElements = testInstance.giveExportedKeys(null, null, "%");
			assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrder(
					new ForeignKeyMetadata("aa", null, schema1Name, "Toto", null, schema1Name, "Tata")
							.addColumn("tata_id", "id"),
					new ForeignKeyMetadata("bb", null, schema2Name, "Tutu", null, schema2Name, "Titi")
							.addColumn("titi_id", "id"));
		}
		
		
		@Test
		void giveColumns_withSchemaCriteria_shouldRespectCriteria() throws SQLException {
			
			DataSource dataSource = new MariaDBDataSource(mariadb);
			
			Connection connection = dataSource.getConnection();
			String schemaName = "giveColumns_schema";
			connection.createStatement().execute("create schema " + schemaName);
			connection.createStatement().execute("create table " + schemaName + ".Toto(name varchar(200))");
			
			MariaDBMetadataReader testInstance = new MariaDBMetadataReader(connection.getMetaData());
			
			Set<ColumnMetadata> ddlElements = testInstance.giveColumns(null, schemaName, "%");
			assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator().containsExactly(
					new ColumnMetadata(null, schemaName, "Toto")
							.setName("name")
							.setSqlType(JDBCType.VARCHAR)
							.setVendorType("VARCHAR")
							.setSize(200)
							.setNullable(true)
							.setAutoIncrement(false)
							.setPosition(1)
							.setPrecision(null)
			);
		}
		
		@Test
		void giveColumns() throws SQLException, IOException {
			
			DataSource dataSource = new MariaDBDataSource(mariadb);
			
			String databaseSchemaCreationScript = new String(IOs.toByteArray(getClass().getResourceAsStream("pizza_delivery_schema.sql")), StandardCharsets.UTF_8);
			
			executeScript(databaseSchemaCreationScript, dataSource.getConnection());
			
			MariaDBMetadataReader testInstance = new MariaDBMetadataReader(dataSource.getConnection().getMetaData());
			Set<ColumnMetadata> ddlElements = testInstance.giveColumns(null, null, "Comm%");
			String schemaName = mariadb.getDatabaseName();
			assertThat(ddlElements)
					.usingRecursiveFieldByFieldElementComparator().containsExactly(
							new ColumnMetadata(null, schemaName, "Command")
									.setName("id")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(true)
									.setPosition(1)
									.setPrecision(0),
							new ColumnMetadata(null, schemaName, "Command")
									.setName("customerId")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(2)
									.setPrecision(0),
							new ColumnMetadata(null, schemaName, "Command")
									.setName("date")
									.setSqlType(JDBCType.TIMESTAMP)
									.setVendorType("DATETIME")
									.setSize(19)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(3)
									.setPrecision(null),
							new ColumnMetadata(null, schemaName, "Command")
									.setName("totalPrice")
									.setSqlType(JDBCType.DECIMAL)
									.setVendorType("DECIMAL")
									.setSize(5)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(4)
									.setPrecision(2),
							new ColumnMetadata(null, schemaName, "Command")
									.setName("status")
									.setSqlType(JDBCType.VARCHAR)
									.setVendorType("ENUM")
									.setSize(9)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(5)
									.setPrecision(null),
							
							
							new ColumnMetadata(null, schemaName, "CommandDetail")
									.setName("commandId")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(1)
									.setPrecision(0),
							new ColumnMetadata(null, schemaName, "CommandDetail")
									.setName("pizzaId")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(2)
									.setPrecision(0),
							new ColumnMetadata(null, schemaName, "CommandDetail")
									.setName("quantity")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(3)
									.setPrecision(0)
					);
		}
		
		public void executeScript(String scriptFile, Connection connection) throws SQLException {
			StringBuilder buffer = new StringBuilder();
			Scanner scanner = new Scanner(scriptFile);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				buffer.append(line);
				// If we encounter a semicolon, then that's a complete statement, so run it
				if (line.endsWith(";")) {
					String command = buffer.toString();
					connection.createStatement().execute(command);
					buffer.setLength(0);
				} else { // Otherwise, just append a newline and keep scanning the file.
					buffer.append("\n");
				}
			}
		}
	}
}