package org.codefilarete.jumper.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.jumper.schema.PostgreSQLSchemaElementCollector.PostgreSQLSchema;
import org.codefilarete.jumper.schema.PostgreSQLSchemaElementCollector.PostgreSQLSchema.Sequence;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.function.Predicates;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgreSQLSchemaElementCollectorTest extends PostgreSQLTest {
	
	@Test
	void build_sequences() throws SQLException {
		
		UrlAwareDataSource dataSourceReference = new PostgreSQLDataSource(postgresql);
		Connection connection = dataSourceReference.getConnection();
		connection.setAutoCommit(false);	// autocommit is true by default
		connection.prepareStatement("create schema dummy_schema").execute();
		connection.prepareStatement("set schema 'dummy_schema'").execute();
		connection.prepareStatement("create sequence DUMMY_SEQUENCE start with 12 increment by 2").execute();
		connection.commit();
		
		PostgreSQLSchemaElementCollector testInstance = new PostgreSQLSchemaElementCollector(dataSourceReference.getConnection().getMetaData());
		PostgreSQLSchema schema = testInstance.withCatalog(null)
				.withSchema("dummy%")
				.withTableNamePattern("%")
				.collect();
	
		// Checking sequences
		PostgreSQLSchema expectedResult = new PostgreSQLSchema(null);
		expectedResult.addSequence("dummy_sequence");
		
		java.util.Comparator<Sequence> sequenceComparator = Predicates.toComparator(Predicates.and(Sequence::getName));
		assertThat(schema.getSequences())
				.usingElementComparator(sequenceComparator)
				.isEqualTo(expectedResult.getSequences());
	}
}