package org.codefilarete.jumper.schema.metadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.ProcedureMetadata.ProcedureType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HSQLDBSequenceMetadataReaderTest extends DefaultMetadataReaderTest {
	
	protected DefaultMetadataReader buildTestInstance() throws SQLException {
		return new HSQLDBSequenceMetadataReader(dataSource.getConnection().getMetaData());
	}
	
	@Test
	void giveProcedures() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.createStatement().execute("create schema " + METADATA_READER_TEST_SCHEMA);
		connection.createStatement().execute("create table " + METADATA_READER_TEST_SCHEMA + ".Toto(id int, firstname varchar(200), lastname varchar(200))");
		// Creating some stupid procedure and function in HSQLDB
		connection.createStatement().execute("create function " + METADATA_READER_TEST_SCHEMA + ".DoSomething(in input_val int)\n" +
				"returns integer\n" +
				"begin atomic\n" +
				"  return 1;\n" +
				"end");
		connection.createStatement().execute("create procedure " + METADATA_READER_TEST_SCHEMA + ".ReadSomething(in input_val int)\n" +
				"reads sql data\n" +
				"dynamic result sets 1\n" +
				"begin atomic\n" +
				"  declare result cursor with return for select id from Toto;\n" +
				"  open result;\n" +
				"end");
		
		DefaultMetadataReader testInstance = buildTestInstance();
		Set<ProcedureMetadata> ddlElements = testInstance.giveProcedures(null, METADATA_READER_TEST_SCHEMA.toUpperCase(), "%");
		
		ProcedureMetadata expectedMetadata1 = new ProcedureMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "DOSOMETHING");
		expectedMetadata1.setSpecificName("DOSOMETHING_10092");
		expectedMetadata1.setType(ProcedureType.FUNCTION);
		
		ProcedureMetadata expectedMetadata2 = new ProcedureMetadata("PUBLIC", METADATA_READER_TEST_SCHEMA.toUpperCase(), "READSOMETHING");
		expectedMetadata2.setSpecificName("READSOMETHING_10094");
		expectedMetadata2.setType(ProcedureType.PROCEDURE);
		
		assertThat(ddlElements).usingRecursiveFieldByFieldElementComparator()
				.containsExactlyInAnyOrder(expectedMetadata1, expectedMetadata2);
	}
}