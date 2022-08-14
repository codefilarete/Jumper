package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DropTableHandlerTest {
	
	@Test
	void generateScript() {
		DropTableHandler testInstance = new DropTableHandler();
		DropTable dropTable = new DropTable("toto");
		assertThat(testInstance.generateScript(dropTable)).isEqualTo("drop table toto");
	}
	
	@Test
	void generateScript_catalogAndSchema() {
		DropTableHandler testInstance = new DropTableHandler();
		DropTable dropTable = new DropTable("toto").setCatalogName("catalog").setSchemaName("schema");
		assertThat(testInstance.generateScript(dropTable)).isEqualTo("drop table catalog.schema.toto");
	}
	
}