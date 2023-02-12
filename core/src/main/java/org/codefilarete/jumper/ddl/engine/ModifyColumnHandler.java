package org.codefilarete.jumper.ddl.engine;

import java.util.Collections;
import java.util.Set;

import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;
import org.codefilarete.jumper.ddl.dsl.support.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Arrays;

/**
 * Default implementation of {@link NewTableGenerator}
 *
 * @author Guillaume Mary
 */
public class ModifyColumnHandler implements ModifyColumnGenerator {
	
	private static final Set<String> MARIADB_KEYWORDS = Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER,
			"ACCESSIBLE", "ANALYZE", "ASENSITIVE",
			"BEFORE", "BIGINT", "BINARY", "BLOB",
			"CALL", "CHANGE", "CONDITION",
			"DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DELAYED", "DETERMINISTIC", "DISTINCTROW", "DIV", "DUAL",
			"EACH", "ELSEIF", "ENCLOSED", "ESCAPED", "EXIT", "EXPLAIN",
			"FLOAT4", "FLOAT8", "FORCE", "FULLTEXT",
			"HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
			"IF", "IGNORE", "INFILE", "INOUT", "INT1", "INT2", "INT3", "INT4", "INT8", "ITERATE",
			"KEY", "KEYS", "KILL",
			"LEAVE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
			"MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NO_WRITE_TO_BINLOG",
			"OPTIMIZE", "OPTIONALLY", "OUT", "OUTFILE",
			"PURGE",
			"RANGE", "READS", "READ_ONLY", "READ_WRITE", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RETURN", "RLIKE",
			"SCHEMAS", "SECOND_MICROSECOND", "SENSITIVE", "SEPARATOR", "SHOW", "SPATIAL", "SPECIFIC", "SQLEXCEPTION", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN",
			"TERMINATED", "TINYBLOB", "TINYINT", "TINYTEXT", "TRIGGER",
			"UNDO", "UNLOCK", "UNSIGNED", "USE", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
			"VARBINARY", "VARCHARACTER",
			"WHILE",
			"X509", "XOR",
			"YEAR_MONTH",
			"ZEROFILL",
			"GENERAL",
			"IGNORE_SERVER_IDS",
			"MASTER_HEARTBEAT_PERIOD",
			"MAXVALUE",
			"RESIGNAL",
			"SIGNAL",
			"SLOW");
	
	private final Set<String> keywords;
	
	public ModifyColumnHandler() {
		this(Collections.emptySet());
	}
	
	public ModifyColumnHandler(Set<String> keywords) {
		this.keywords = keywords;
	}
	
	@Override
	public String generateScript(ModifyColumn modifyColumn) {
		DDLAppender sqlAlterTable = new DDLAppender("alter table ");
		sqlAlterTable.catIf(!Strings.isEmpty(modifyColumn.getCatalogName()), modifyColumn.getCatalogName(), ".")
				.catIf(!Strings.isEmpty(modifyColumn.getSchemaName()), modifyColumn.getSchemaName(), ".");
		
		sqlAlterTable.cat(modifyColumn.getTable(), " modify column ");
		generateCreateColumn(modifyColumn, sqlAlterTable);
		return sqlAlterTable.toString();
	}
	
	protected void generateCreateColumn(ModifyColumn column, DDLAppender sqlAlterTable) {
		sqlAlterTable.cat(column, " ", column.getSqlType())
				.catIf(column.getExtraArguments() != null, " ", column.getExtraArguments())
				.catIf(!column.isNullable(), " not null")
				.catIf(column.isAutoIncrement(), " auto_increment")
				.catIf(column.getDefaultValue() != null, " default ", column.getDefaultValue())
		;
	}
	
	
	/**
	 * A {@link StringAppender} that automatically escapes {@link Table} and {@link ModifyColumn} names against reserved words
	 */
	private class DDLAppender extends StringAppender {
		
		public DDLAppender(Object... o) {
			super(o);
		}
		
		/**
		 * Overridden to append {@link DDLAppender} names
		 *
		 * @param o any object
		 * @return this
		 */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof ModifyColumn) {
				String columnName = ((ModifyColumn) o).getName();
				if (keywords.contains(columnName)) {
					columnName = "`" + columnName + "`";
				}
				return super.cat(columnName);
			} else if (o instanceof Table) {
				String tableName = ((Table) o).getName();
				if (keywords.contains(tableName)) {
					tableName = "`" + tableName + "`";
				}
				return super.cat(tableName);
			} else {
				return super.cat(o);
			}
		}
	}
}
