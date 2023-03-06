package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.codefilarete.jumper.schema.MySQLDataSource;
import org.codefilarete.jumper.schema.MySQLTest;
import org.codefilarete.tool.io.IOs;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MySQLMetadataReaderTest {
	
	@Nested
	class IntegrationTest extends MySQLTest {
		
		@Test
		void giveSequences() throws SQLException {
			
			DataSource dataSource = new MySQLDataSource(mysqldb);
			
			MySQLMetadataReader testInstance = new MySQLMetadataReader(dataSource.getConnection().getMetaData());
			Set<SequenceMetadata> ddlElements = testInstance.giveSequences(null, null);
			ddlElements.forEach(t -> {
				System.out.println(t.getName());
			});
		}
		
		@Test
		void giveColumns() throws SQLException, IOException {
			
			DataSource dataSource = new MySQLDataSource(mysqldb);
			
			String databaseSchemaCreationScript = new String(IOs.toByteArray(getClass().getResourceAsStream("pizza_delivery_schema.sql")), StandardCharsets.UTF_8);
			
			executeScript(databaseSchemaCreationScript, dataSource.getConnection());
			
			MySQLMetadataReader testInstance = new MySQLMetadataReader(dataSource.getConnection().getMetaData());
			Set<ColumnMetadata> ddlElements = testInstance.giveColumns("", null, "Comm%");
			String schemaName = mysqldb.getDatabaseName();
			Assertions.assertThat(ddlElements)
					.usingRecursiveFieldByFieldElementComparator().containsExactly(
							new ColumnMetadata(schemaName, null, "Command")
									.setName("id")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(true)
									.setPosition(1)
									.setPrecision(0),
							new ColumnMetadata(schemaName, null, "Command")
									.setName("customerId")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(2)
									.setPrecision(0),
							new ColumnMetadata(schemaName, null, "Command")
									.setName("date")
									.setSqlType(JDBCType.TIMESTAMP)
									.setVendorType("DATETIME")
									.setSize(19)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(3)
									.setPrecision(null),
							new ColumnMetadata(schemaName, null, "Command")
									.setName("totalPrice")
									.setSqlType(JDBCType.DECIMAL)
									.setVendorType("DECIMAL")
									.setSize(5)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(4)
									.setPrecision(2),
							new ColumnMetadata(schemaName, null, "Command")
									.setName("status")
									.setSqlType(JDBCType.VARCHAR)
									.setVendorType("ENUM")
									.setSize(9)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(5)
									.setPrecision(null),
							
							
							new ColumnMetadata(schemaName, null, "CommandDetail")
									.setName("commandId")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(1)
									.setPrecision(0),
							new ColumnMetadata(schemaName, null, "CommandDetail")
									.setName("pizzaId")
									.setSqlType(JDBCType.INTEGER)
									.setVendorType("INT")
									.setSize(10)
									.setNullable(false)
									.setAutoIncrement(false)
									.setPosition(2)
									.setPrecision(0),
							new ColumnMetadata(schemaName, null, "CommandDetail")
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