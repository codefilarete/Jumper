package org.codefilarete.jumper.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.jumper.schema.HSQLDBSchemaElementCollector.HSQLDBSchema;
import org.codefilarete.jumper.schema.HSQLDBSchemaElementCollector.HSQLDBSchema.Sequence;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.function.Predicates;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HSQLDBSchemaElementCollectorTest {
	
	@Test
	void build_sequences() throws SQLException {
		
		UrlAwareDataSource dataSourceReference = new HSQLDBInMemoryDataSource();
		Connection connection = dataSourceReference.getConnection();
		connection.prepareStatement("create schema dummy_schema").execute();
		connection.prepareStatement("set schema dummy_schema").execute();
		connection.prepareStatement("create sequence DUMMY_SEQUENCE start with 12 increment by 2").execute();
		connection.commit();
		
		HSQLDBSchemaElementCollector testInstance = new HSQLDBSchemaElementCollector(dataSourceReference.getConnection().getMetaData());
		HSQLDBSchema schema = testInstance.withCatalog(null)
				.withSchema("DUMMY%")
				.withTableNamePattern("%")
				.collect();
	
		// Checking sequences
		HSQLDBSchema expectedResult = new HSQLDBSchema(null);
		expectedResult.addSequence("DUMMY_SEQUENCE");
		
		java.util.Comparator<Sequence> sequenceComparator = Predicates.toComparator(Predicates.and(Sequence::getName));
		assertThat(schema.getSequences())
				.usingElementComparator(sequenceComparator)
				.isEqualTo(expectedResult.getSequences());
	}
}