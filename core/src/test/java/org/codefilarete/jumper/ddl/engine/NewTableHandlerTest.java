package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.DDLEase;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.junit.jupiter.api.Nested;
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
				.setPrimaryKey("col1", "col2")
				.build();
		assertThat(testInstance.generateScript(newTableComposedPK)).isEqualTo("create table toto("
				+ "col1 varchar(100), "
				+ "col2 bigint, primary key (col1, col2))");
	}
	
	@Nested
	class UniqueConstraint {
		@Test
		void generateScript_ucOnColumn() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint").unique()
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint unique)");
		}
		
		@Test
		void generateScript_ucOnColumn_withName() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint").unique("uniqueConstraintName")
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "constraint uniqueConstraintName unique (col2))");
		}
		
		@Test
		void generateScript_ucOnColumn_withSameNameOnSeveral() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTable2SinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.unique("UC")
					.addColumn("col2", "bigint")
					.unique("UC")
					.build();
			assertThat(testInstance.generateScript(newTable2SinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "constraint UC unique (col1, col2))");
			
		}
		
		@Test
		void generateScript_ucOnTable() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableComposedPK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint")
					.addUniqueConstraint("col1", "col2").setUniqueConstraintName("UC")
					.build();
			assertThat(testInstance.generateScript(newTableComposedPK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "constraint UC unique (col1, col2))");
		}
		
		@Test
		void generateScript_ucOnTableWithoutName() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableComposedPK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint")
					.addUniqueConstraint("col1", "col2")
					.build();
			assertThat(testInstance.generateScript(newTableComposedPK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "unique (col1, col2))");
		}
		
		@Test
		void generateScript_orderIsKept() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.unique("dummyConstraintName")
					.addColumn("col2", "bigint")
					.unique()
					.addColumn("col3", "bigint")
					.unique("dummyConstraintName")
					.addUniqueConstraint("a", "b", "c").setUniqueConstraintName("myConstraintName")
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint unique, "
					+ "col3 bigint, "
					+ "constraint dummyConstraintName unique (col1, col3), "
					+ "constraint myConstraintName unique (a, b, c))"
			);
		}
	}
	
	@Nested
	class ForeignKeyConstraint {
		
		@Test
		void generateScript_fkOnColumn() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint")
					.references("dummyForeignTable", "externalCol")
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "foreign key (col2) references dummyForeignTable(externalCol))");
		}
		
		@Test
		void generateScript_fkOnColumn_withName() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint")
							.references("dummyForeignTable", "externalCol", "FK_NAME")
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "constraint FK_NAME foreign key (col2) references dummyForeignTable(externalCol))");
		}
		
		@Test
		void generateScript_fkOnColumn_withSameNameOnSeveral() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
							.references("dummyForeignTable", "externalCol1", "FK_NAME")
					.addColumn("col2", "bigint")
							.references("dummyForeignTable", "externalCol2", "FK_NAME")
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "constraint FK_NAME foreign key (col1, col2) references dummyForeignTable(externalCol1, externalCol2))");
		}
		
		@Test
		void generateScript_fkOnTable() {
			NewTableHandler testInstance = new NewTableHandler();
			
			NewTable newTable2SinglePK = DDLEase.createTable("toto")
					.addColumn("col1", "varchar(100)")
					.addColumn("col2", "bigint")
					.addForeignKey("dummyForeignTable")
						.setForeignKeyName("FK_col2")
						.addColumnReference("a", "aa")
					.build();
			assertThat(testInstance.generateScript(newTable2SinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "constraint FK_col2 foreign key (a) references dummyForeignTable(aa))");
		}
		
		@Test
		void generateScript_orderIsKept() {
			NewTableHandler testInstance = new NewTableHandler();
			NewTable newTableSinglePK = DDLEase.createTable("toto")
					.addForeignKey("dummyForeignTable")
						.addColumnReference("a", "aa")
					.addColumn("col1", "varchar(100)")
						.references("dummyForeignTable", "externalCol1", "FK_NAME")
					.addColumn("col2", "bigint")
						.references("dummyForeignTable", "externalCol2")
					.addColumn("col3", "bigint")
						.references("dummyForeignTable", "externalCol3", "FK_NAME")
					.addForeignKey("dummyForeignTable")
						.setForeignKeyName("FK_col2")
						.addColumnReference("b", "bb")
					.build();
			assertThat(testInstance.generateScript(newTableSinglePK)).isEqualTo("create table toto("
					+ "col1 varchar(100), "
					+ "col2 bigint, "
					+ "col3 bigint, "
					+ "foreign key (a) references dummyForeignTable(aa), "
					+ "constraint FK_NAME foreign key (col1, col3) references dummyForeignTable(externalCol1, externalCol3), "
					+ "foreign key (col2) references dummyForeignTable(externalCol2), "
					+ "constraint FK_col2 foreign key (b) references dummyForeignTable(bb))"
			);
		}
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