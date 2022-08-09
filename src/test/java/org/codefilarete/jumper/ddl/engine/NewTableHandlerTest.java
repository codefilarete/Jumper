package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.DDLEase;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class NewTableHandlerTest {
	
	@Test
	void generateScript() {
		NewTableHandler testInstance = new NewTableHandler();
		NewTable newTable = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.notNull()
					.autoIncrement()
					.defaultValue("'hello world !'")
				.addColumn("col2", "bigint")
				.build();
		assertEquals("create table toto(" 
				+ "col1 varchar(100) not null auto_increment default 'hello world !', " 
				+ "col2 bigint)", testInstance.generateScript(newTable));
	}
	
	@Test
	void generateScript_primaryKey() {
		NewTableHandler testInstance = new NewTableHandler();
		NewTable newTableSinglePK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.inPrimaryKey()
				.addColumn("col2", "bigint")
				.build();
		assertEquals("create table toto(" 
				+ "col1 varchar(100), " 
				+ "col2 bigint, primary key (col1))", testInstance.generateScript(newTableSinglePK));
		
		NewTable newTableComposedPK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.inPrimaryKey()
				.addColumn("col2", "bigint")
					.inPrimaryKey()
				.build();
		assertEquals("create table toto(" 
				+ "col1 varchar(100), " 
				+ "col2 bigint, primary key (col1, col2))", testInstance.generateScript(newTableComposedPK));
	}
	
	@Test
	void generateScript_uniqueConstraint() {
		NewTableHandler testInstance = new NewTableHandler();
		NewTable newTableSinglePK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.inPrimaryKey()
				.addColumn("col2", "bigint").uniqueConstraint("UK_col2")
				.build();
		assertEquals("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, primary key (col1), "
				+ "constraint UK_col2 unique (col2))", testInstance.generateScript(newTableSinglePK));
		
		NewTable newTableComposedPK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.inPrimaryKey()
					.uniqueConstraint("UK_col1")
				.addColumn("col2", "bigint")
					.inPrimaryKey()
					.uniqueConstraint("UK_col2")
				.build();
		assertEquals("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, primary key (col1, col2), "
				+ "constraint UK_col1 unique (col1), "
				+ "constraint UK_col2 unique (col2))", testInstance.generateScript(newTableComposedPK));
	}
	
	@Test
	void generateScript_catalogAndSchema() {
		NewTableHandler testInstance = new NewTableHandler();
		NewTable newTable = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
				.addColumn("col2", "bigint")
				.setSchema("schema")
				.setCatalog("catalog")
				.build();
		assertEquals("create table catalog.schema.toto(" 
				+ "col1 varchar(100), " 
				+ "col2 bigint)", testInstance.generateScript(newTable));
		
	}
	
}