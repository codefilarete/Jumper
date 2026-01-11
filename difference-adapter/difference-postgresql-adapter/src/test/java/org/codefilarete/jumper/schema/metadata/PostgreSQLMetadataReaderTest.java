package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.jumper.schema.PostgreSQLDataSource;
import org.codefilarete.jumper.schema.PostgreSQLTest;
import org.junit.jupiter.api.Test;

class PostgreSQLMetadataReaderTest extends PostgreSQLTest {
	
	@Test
	void giveSequences() throws SQLException {
		
		DataSource dataSource = new PostgreSQLDataSource(postgresql);
		
		PostgreSQLMetadataReader testInstance = new PostgreSQLMetadataReader(dataSource.getConnection().getMetaData());
		Set<SequenceMetadata> ddlElements = testInstance.giveSequences(null, null);
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
}