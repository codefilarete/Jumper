package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

class HSQLDBSequenceMetadataReaderTest {
	
	@Test
	void giveSequences() throws SQLException {
		
		DataSource dataSource = new HSQLDBInMemoryDataSource();
		
		HSQLDBSequenceMetadataReader testInstance = new HSQLDBSequenceMetadataReader(dataSource.getConnection().getMetaData());
		Set<SequenceMetadata> ddlElements = testInstance.giveSequences(null, null);
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
}