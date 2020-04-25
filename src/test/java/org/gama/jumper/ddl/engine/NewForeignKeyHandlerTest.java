package org.gama.jumper.ddl.engine;

import org.gama.jumper.ddl.dsl.DDLEase;
import org.gama.jumper.ddl.dsl.support.NewForeignKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class NewForeignKeyHandlerTest {
	
	@Test
	void generateScript() {
		NewForeignKeyHandler testInstance = new NewForeignKeyHandler();
		NewForeignKey newIndex = DDLEase.createForeignKey("toto", "tutu")
				.addSourceColumn("col1")
				.addSourceColumn("col2")
				.targetTable("tata")
				.addTargetColumn("col1_")
				.addTargetColumn("col2_")
				.build();
		assertEquals("alter table tutu add constraint toto foreign key(col1, col2) references tata(col1_, col2_)", testInstance.generateScript(newIndex));
	}
	
	@Test
	void generateScript_catalogAndSchema() {
		NewForeignKeyHandler testInstance = new NewForeignKeyHandler();
		NewForeignKey newIndex = DDLEase.createForeignKey("toto", "tutu")
				.setSchema("schema")
				.setCatalog("catalog")
				.addSourceColumn("col1")
				.addSourceColumn("col2")
				.targetTable("tata")
				.addTargetColumn("col1_")
				.addTargetColumn("col2_")
				.build();
		assertEquals("alter table catalog.schema.tutu add constraint toto foreign key(col1, col2) references catalog.schema.tata(col1_, col2_)", testInstance.generateScript(newIndex));
	}
	
}