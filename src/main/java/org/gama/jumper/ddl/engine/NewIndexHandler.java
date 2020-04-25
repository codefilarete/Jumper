package org.gama.jumper.ddl.engine;

import org.gama.jumper.ddl.dsl.support.Column;
import org.gama.jumper.ddl.dsl.support.NewIndex;
import org.gama.jumper.ddl.dsl.support.Table;
import org.gama.lang.StringAppender;
import org.gama.lang.Strings;

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
	 * A {@link StringAppender} that automatically appends {@link Column} and {@link Table}
	 */
	private static class DDLAppender extends StringAppender {
		
		public DDLAppender(Object... o) {
			// we don't all super(o) because it may need dmlNameProvider
			cat(o);
		}
		
		/**
		 * Overriden to append {@link Column} names
		 *
		 * @param o any object
		 * @return this
		 */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Column) {
				Column column = (Column) o;
				catIf(!Strings.isEmpty(column.getCatalogName()), column.getCatalogName(), ".");
				catIf(!Strings.isEmpty(column.getSchemaName()), column.getSchemaName(), ".");
				return super.cat(column.getName());
			} else if (o instanceof Table) {
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
