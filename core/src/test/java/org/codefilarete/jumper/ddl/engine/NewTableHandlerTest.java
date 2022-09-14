package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.DDLEase;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
		assertThat(testInstance.generateScript(newTable)).isEqualTo("create table toto("
				+ "col1 varchar(100) not null auto_increment default 'hello world !', "
				+ "col2 bigint)");
	}
	
	@Test
	void generateScript_primaryKey() {
		NewTableHandler testInstance = new NewTableHandler();
		NewTable newTableSinglePK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.primaryKey()
				.addColumn("col2", "bigint")
				.build();
		assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, primary key (col1))");
		
		NewTable newTableComposedPK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
				.addColumn("col2", "bigint")
				.primaryKey("col1", "col2")
				.build();
		assertThat(testInstance.generateScript(newTableComposedPK)).isEqualTo("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, primary key (col1, col2))");
	}
	
	@Test
	void generateScript_uniqueConstraint() {
		NewTableHandler testInstance = new NewTableHandler();
		NewTable newTableSinglePK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
				.addColumn("col2", "bigint").uniqueConstraint("UK_col2")
				.build();
		assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, "
				+ "constraint UK_col2 unique (col2))");
		
		NewTable newTable2SinglePK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
					.uniqueConstraint("UK_col1")
				.addColumn("col2", "bigint")
					.uniqueConstraint("UK_col2")
				.build();
		assertThat(testInstance.generateScript(newTable2SinglePK)).isEqualTo("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, "
				+ "constraint UK_col1 unique (col1), "
				+ "constraint UK_col2 unique (col2))");
		
		NewTable newTableComposedPK = DDLEase.createTable("toto")
				.addColumn("col1", "varchar(100)")
				.addColumn("col2", "bigint")
				.uniqueConstraint("UK", "col1", "col2")
				.build();
		assertThat(testInstance.generateScript(newTableComposedPK)).isEqualTo("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, "
				+ "constraint UK unique (col1, col2))");
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
		assertThat(testInstance.generateScript(newTable)).isEqualTo("create table catalog.schema.toto("
				+ "col1 varchar(100), "
				+ "col2 bigint)");
		
	}
	
}