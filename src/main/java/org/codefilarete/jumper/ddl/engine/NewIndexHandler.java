package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

/**
 * @author Guillaume Mary
 */
public class NewIndexHandler implements NewIndexGenerator {
	
	@Override
	public String generateScript(NewIndex index) {
		Table table = index.getTable();
		StringAppender sqlCreateIndex = new DDLAppender("create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ")
				.cat(table, "(")
				.ccat(index.getColumns(), ", ")
				.cat(")");
		return sqlCreateIndex.toString();
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
				Table table = ((Table) o);
				catIf(!Strings.isEmpty(table.getCatalogName()), table.getCatalogName(), ".");
				catIf(!Strings.isEmpty(table.getSchemaName()), table.getSchemaName(), ".");
				return super.cat(table.getName());
			} else {
				return super.cat(o);
			}
		}
	}
	
}
