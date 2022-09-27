package org.codefilarete.jumper.schema.metadata;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Set;

import org.codefilarete.jumper.schema.MariaDBDataSource;
import org.codefilarete.jumper.schema.MariaDBTest;
import org.junit.jupiter.api.Test;

class MariaDBSequenceMetadataReaderTest extends MariaDBTest {
	
	@Test
	void giveSequences() throws SQLException {
		
		DataSource dataSource = new MariaDBDataSource(mariadb);
		
		MariaDBSequenceMetadataReader testInstance = new MariaDBSequenceMetadataReader(dataSource.getConnection().getMetaData());
		Set<SequenceMetadata> ddlElements = testInstance.giveSequences(null, null);
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}
}