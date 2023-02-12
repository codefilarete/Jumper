package org.codefilarete.jumper.ddl.engine;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewColumn;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewPrimaryKey;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewUniqueConstraint;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Arrays;

/**
 * Default implementation of {@link NewTableGenerator}
 *
 * @author Guillaume Mary
 */
public class NewTableHandler implements NewTableGenerator {
	
	public static final Set<String> MARIADB_KEYWORDS = Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER,
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
	
	public NewTableHandler() {
		this(Collections.emptySet());
	}
	
	public NewTableHandler(Set<String> keywords) {
		this.keywords = keywords;
	}
	
	@Override
	public String generateScript(NewTable table) {
		DDLAppender sqlCreateTable = new DDLAppender("create table ");
		sqlCreateTable.catIf(!Strings.isEmpty(table.getCatalogName()), table.getCatalogName(), ".")
				.catIf(!Strings.isEmpty(table.getSchemaName()), table.getSchemaName(), ".");
		
		sqlCreateTable.cat(table.getName(), "(");
		Set<NewColumn> uniqueKeyConstraints = new LinkedHashSet<>();
		for (NewColumn column : table.getColumns()) {
			generateCreateColumn(column, sqlCreateTable);
			sqlCreateTable.cat(", ");
			if (column.getUniqueConstraintName() != null) {
				uniqueKeyConstraints.add(column);
			}
		}
		sqlCreateTable.cutTail(2);
		if (table.getPrimaryKey() != null) {
			generateCreatePrimaryKey(table.getPrimaryKey(), sqlCreateTable);
		}
		uniqueKeyConstraints.forEach(columnWithUKConstraint -> generateUniqueConstraint(columnWithUKConstraint, sqlCreateTable));
		table.getUniqueConstraints().forEach(uk -> generateUniqueConstraint(uk, sqlCreateTable));
		sqlCreateTable.cat(")");
		return sqlCreateTable.toString();
	}
	
	protected void generateUniqueConstraint(NewColumn columnWithUKConstraint, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", constraint ",
				columnWithUKConstraint.getUniqueConstraintName(),
				" unique (", columnWithUKConstraint, ")");
	}
	
	protected void generateUniqueConstraint(NewUniqueConstraint uniqueConstraint, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", constraint ",
				uniqueConstraint.getName(),
				" unique (").ccat(uniqueConstraint.getColumns(), ", ").cat(")");
	}
	
	protected void generateCreatePrimaryKey(NewPrimaryKey primaryKey, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", primary key (")
				.ccat(primaryKey.getColumns(), ", ")
				.cat(")");
	}
	
	protected void generateCreateColumn(NewColumn column, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(column, " ", column.getSqlType())
				.catIf(column.getExtraArguments() != null, " ", column.getExtraArguments())
				.catIf(!column.isNullable(), " not null")
				.catIf(column.isAutoIncrement(), " auto_increment")
				.catIf(column.getDefaultValue() != null, " default ", column.getDefaultValue())
		;
	}
	
	
	/**
	 * A {@link StringAppender} that automatically appends {@link NewColumn} names
	 */
	private class DDLAppender extends StringAppender {
		
		public DDLAppender(Object... o) {
			super(o);
		}
		
		/**
		 * Overridden to append {@link NewColumn} names
		 *
		 * @param o any object
		 * @return this
		 */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof NewColumn) {
				String columnName = ((NewColumn) o).getName();
				if (keywords.contains(columnName)) {
					columnName = "`" + columnName + "`";
				}
				return super.cat(columnName);
			} else {
				return super.cat(o);
			}
		}
	}
}
