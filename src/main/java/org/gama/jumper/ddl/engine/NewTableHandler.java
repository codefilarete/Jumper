package org.codefilarete.jumper.ddl.engine;

import javax.annotation.Nonnull;

import org.codefilarete.jumper.ddl.dsl.support.NewColumn;
import org.codefilarete.jumper.ddl.dsl.support.NewPrimaryKey;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

/**
 * Default implementation of {@link NewTableGenerator}
 * 
 * @author Guillaume Mary
 */
public class NewTableHandler implements NewTableGenerator {
	
	@Override
	public String generateScript(NewTable table) {
		DDLAppender sqlCreateTable = new DDLAppender("create table ");
		sqlCreateTable.catIf(!Strings.isEmpty(table.getCatalogName()), table.getCatalogName(), ".")
				.catIf(!Strings.isEmpty(table.getSchemaName()), table.getSchemaName(), ".");
		
		sqlCreateTable.cat(table.getName(), "(");
		for (NewColumn column : table.getColumns()) {
			generateCreateColumn(column, sqlCreateTable);
			sqlCreateTable.cat(", ");
		}
		sqlCreateTable.cutTail(2);
		if (table.getPrimaryKey() != null) {
			generateCreatePrimaryKey(table.getPrimaryKey(), sqlCreateTable);
		}
		sqlCreateTable.cat(")");
		return sqlCreateTable.toString();
	}
	
	protected void generateCreatePrimaryKey(@Nonnull NewPrimaryKey primaryKey, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", primary key (")
				.ccat(primaryKey.getColumns(), ", ")
				.cat(")");
	}
	
	protected void generateCreateColumn(NewColumn column, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(column, " ", column.getSqlType())
				.catIf(!column.isNullable(), " not null")
				.catIf(column.isAutoIncrement(), " auto_increment")
				.catIf(column.getDefaultValue() != null, " default ", column.getDefaultValue())
		;
	}
	
	
	/**
	 * A {@link StringAppender} that automatically appends {@link NewColumn} names
	 */
	private static class DDLAppender extends StringAppender {
		
		public DDLAppender(Object... o) {
			// we don't all super(o) because it may need dmlNameProvider
			cat(o);
		}
		
		/**
		 * Overriden to append {@link NewColumn} names
		 *
		 * @param o any object
		 * @return this
		 */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof NewColumn) {
				return super.cat(((NewColumn) o).getName());
			} else {
				return super.cat(o);
			}
		}
	}
	
//	
//	public String generateCreateForeignKey(ForeignKey foreignKey) {
//		Table table = foreignKey.getTable();
//		StringAppender sqlCreateFK = new DDLAppender(dmlNameProvider, "alter table ", table)
//				.cat(" add constraint ", foreignKey.getName(), " foreign key(")
//				.ccat(foreignKey.getColumns(), ", ")
//				.cat(") references ", foreignKey.getTargetTable(), "(")
//				.ccat(foreignKey.getTargetColumns(), ", ");
//		return sqlCreateFK.cat(")").toString();
//	}
//	
//	public String generateAddColumn(Column column) {
//		DDLAppender sqladdColumn = new DDLAppender(dmlNameProvider, "alter table ", column.getTable(), " add column ", column, " ", getSqlType(column));
//		return sqladdColumn.toString();
//	}
//	
//	public String generateDropTable(Table table) {
//		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "drop table ", table);
//		return sqlCreateTable.toString();
//	}
//	
//	public String generateDropTableIfExists(Table table) {
//		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "drop table if exists ", table);
//		return sqlCreateTable.toString();
//	}
//	
//	public String generateDropIndex(Index index) {
//		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "drop index ", index.getName());
//		return sqlCreateTable.toString();
//	}
//	
//	public String generateDropForeignKey(ForeignKey foreignKey) {
//		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "alter table ", foreignKey.getTable(), " drop constraint ", foreignKey.getName());
//		return sqlCreateTable.toString();
//	}
//	
//	public String generateDropColumn(Column column) {
//		DDLAppender sqlDropColumn = new DDLAppender(dmlNameProvider, "alter table ", column.getTable(), " drop column ", column);
//		return sqlDropColumn.toString();
//	}
	
}
