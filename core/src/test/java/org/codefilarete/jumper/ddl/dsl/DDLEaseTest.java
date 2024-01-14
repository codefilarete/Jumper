package org.codefilarete.jumper.ddl.dsl;

import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class DDLEaseTest {
	
	@Test
	void apiUsage() {
		DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.notNull()
					.autoIncrement()
					.defaultValue("hello world !")
				.addUniqueConstraint("a", "b", "c")
				.addForeignKey("anyTargetTable").setForeignKeyName("titi")
				.addColumnReference("a", "a")
				.addColumnReference("b", "b")
				.addColumn("col2", "bigint")
				.setSchema("schema")
				.setCatalog("catalog");
	}
}