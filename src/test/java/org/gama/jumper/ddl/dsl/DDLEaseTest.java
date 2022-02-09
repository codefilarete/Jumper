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
			.addColumn("col2", "bigint")
			.setSchema("schema")
			.setCatalog("catalog");
	}
	
}