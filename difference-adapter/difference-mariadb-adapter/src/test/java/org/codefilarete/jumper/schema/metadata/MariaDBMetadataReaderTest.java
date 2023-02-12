package org.codefilarete.jumper.schema.metadata;

import com.github.dockerjava.zerodep.shaded.org.apache.commons.codec.Charsets;
import org.codefilarete.jumper.schema.MariaDBDataSource;
import org.codefilarete.jumper.schema.MariaDBTest;
import org.codefilarete.tool.io.IOs;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

class MariaDBMetadataReaderTest extends MariaDBTest {
	
	@Test
	void giveSequences() throws SQLException {
		
		DataSource dataSource = new MariaDBDataSource(mariadb);
		
		MariaDBMetadataReader testInstance = new MariaDBMetadataReader(dataSource.getConnection().getMetaData());
		Set<SequenceMetadata> ddlElements = testInstance.giveSequences(null, null);
		ddlElements.forEach(t -> {
			System.out.println(t.getName());
//			System.out.println(t.getName() + ": " + t.getType());
		});
	}

	@Test
	void giveColumns() throws SQLException, IOException {

		DataSource dataSource = new MariaDBDataSource(mariadb);


		String sakilaDatabaseSchemaScript = new String(IOs.toByteArray(getClass().getResourceAsStream("/sakila-database/sakila-schema.sql")), Charsets.UTF_8);

		System.out.println(sakilaDatabaseSchemaScript);

		MariaDBMetadataReader testInstance = new MariaDBMetadataReader(dataSource.getConnection().getMetaData());
		Set<ColumnMetadata> ddlElements = testInstance.giveColumns("", null, "Patient");
		ddlElements.forEach(t -> {
			System.out.println(t);
//			System.out.println(t.getName() + ": " + t.getSqlType() + Nullable.nullable(t.getSize()).map(s -> "(" + s + ")").getOr(""));
		});
	}
}