package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.DDLEase;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class NewIndexHandlerTest {
	
	@Test
	void generateScript() {
		NewIndexHandler testInstance = new NewIndexHandler();
		NewIndex newIndex = DDLEase.createIndex("toto", "tutu")
				.addColumn("col1")
				.unique()
				.build();
		assertEquals("create unique index toto on tutu(col1)", testInstance.generateScript(newIndex));
	}
	
	@Test
	void generateScript_catalogAndSchema() {
		NewIndexHandler testInstance = new NewIndexHandler();
		NewIndex newIndex = DDLEase.createIndex("toto", "tutu")
				.addColumn("col1")
				.setSchema("schema")
				.setCatalog("catalog")
				.unique()
				.build();
		assertEquals("create unique index toto on catalog.schema.tutu(col1)", testInstance.generateScript(newIndex));
	}
	
}