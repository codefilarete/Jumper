package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

/**
 * @author Guillaume Mary
 */
public class NewForeignKeyHandler implements NewForeignKeyGenerator {
	
	@Override
	public String generateScript(NewForeignKey foreignKey) {
		Table table = foreignKey.getTable();
		Table targetTable = foreignKey.getTargetTable();
		StringAppender sqlCreateFK = new DDLAppender("alter table ", table)
				.cat(" add constraint ", foreignKey.getName(), " foreign key(")
				.ccat(foreignKey.getSourceColumns(), ", ")
				.cat(") references ")
				// we forces target table to be on same catalog and schema that source one, because according to my knowledge no RDBMS supports
				// cross schema integrity reference, may change if one day one supports it !
				.catIf(!Strings.isEmpty(table.getCatalogName()), table.getCatalogName(), ".")
				.catIf(!Strings.isEmpty(table.getSchemaName()), table.getSchemaName(), ".")
				.cat(targetTable.getName(), "(")
				.ccat(foreignKey.getTargetColumns(), ", ");
		return sqlCreateFK.cat(")").toString();
	}
	
	/**
	 * A {@link StringAppender} that automatically appends {@link Table}
	 */
	private static class DDLAppender extends StringAppender {
		
		public DDLAppender(Object... o) {
			super(o);
		}
		
		/**
		 * Overridden to append {@link Table} names
		 *
		 * @param o any object
		 * @return this
		 */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Table) {
				org.codefilarete.jumper.ddl.dsl.support.Table table = ((org.codefilarete.jumper.ddl.dsl.support.Table) o);
				catIf(!Strings.isEmpty(table.getCatalogName()), table.getCatalogName(), ".");
				catIf(!Strings.isEmpty(table.getSchemaName()), table.getSchemaName(), ".");
				return super.cat(table.getName());
			} else {
				return super.cat(o);
			}
		}
	}
	
}
