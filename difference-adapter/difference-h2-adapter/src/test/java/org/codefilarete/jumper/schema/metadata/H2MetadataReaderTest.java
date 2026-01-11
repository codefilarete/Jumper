package org.codefilarete.jumper.schema.metadata;

import java.sql.SQLException;

import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;
import org.codefilarete.tool.Strings;
import org.junit.jupiter.api.BeforeEach;

public class H2MetadataReaderTest extends MetadataReaderTest {
	
	@BeforeEach
	@Override
	void createDataSource() {
		dataSource = new H2InMemoryDataSource();
	}
	
	@Override
	protected String getDefaultCatalog() {
		return Strings.cutHead(((H2InMemoryDataSource) dataSource).getUrl(), "jdbc:h2:mem:".length()).toString().toUpperCase();
	}
	
	@Override
	protected SchemaMetadataReader buildTestInstance() throws SQLException {
		return new H2MetadataReader(dataSource.getConnection().getMetaData());
	}
	
	@Override
	void giveProcedures() throws SQLException {
		// H2 procedure testing if needed
	}
}
