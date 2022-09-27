package org.codefilarete.jumper.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.jumper.schema.MariaDBSchemaElementCollector.MariaDBSchema;
import org.codefilarete.jumper.schema.MariaDBSchemaElementCollector.MariaDBSchema.Sequence;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.function.Predicates;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MariaDBSchemaElementCollectorTest extends MariaDBTest {
	
	@Test
	void build_sequences() throws SQLException {
		
		UrlAwareDataSource dataSourceReference = new MariaDBDataSource(mariadb);
		Connection connection = dataSourceReference.getConnection();
		connection.prepareStatement("create schema dummy_schema").execute();
		connection.prepareStatement("use dummy_schema").execute();
		connection.prepareStatement("create sequence DUMMY_SEQUENCE start with 12 increment by 2").execute();
		connection.commit();
		
		MariaDBSchemaElementCollector testInstance = new MariaDBSchemaElementCollector(dataSourceReference.getConnection().getMetaData());
		MariaDBSchema schema = testInstance.withCatalog(null)
				.withSchema("DUMMY%")
				.withTableNamePattern("%")
				.collect();
	
		// Checking sequences
		MariaDBSchema expectedResult = new MariaDBSchema(null);
		expectedResult.addSequence("DUMMY_SEQUENCE");
		
		java.util.Comparator<Sequence> sequenceComparator = Predicates.toComparator(Predicates.and(Sequence::getName));
		assertThat(schema.getSequences())
				.usingElementComparator(sequenceComparator)
				.isEqualTo(expectedResult.getSequences());
	}
}