package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.DDLEase;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
		assertThat(testInstance.generateScript(newIndex)).isEqualTo("alter table tutu add constraint toto foreign key(col1, col2) references tata(col1_, col2_)");
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
		assertThat(testInstance.generateScript(newIndex)).isEqualTo("alter table catalog.schema.tutu add constraint toto foreign key(col1, col2) references catalog.schema.tata(col1_, col2_)");
	}
	
}