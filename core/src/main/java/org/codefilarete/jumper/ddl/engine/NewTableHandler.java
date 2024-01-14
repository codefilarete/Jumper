package org.codefilarete.jumper.ddl.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewColumn;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewPrimaryKey;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewUniqueConstraint;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Predicates;

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
		for (NewColumn column : table.getColumns()) {
			generateCreateColumn(column, sqlCreateTable);
			sqlCreateTable.cat(", ");
		}
		sqlCreateTable.cutTail(2);
		if (table.getPrimaryKey() != null) {
			generateCreatePrimaryKey(table.getPrimaryKey(), sqlCreateTable);
		}
		
		arrangeUniqueConstraints(table.getUniqueConstraints())
				.forEach(uk -> generateUniqueConstraint(uk, sqlCreateTable));
		
		arrangeForeignKeys(table.getForeignKeys())
				.forEach(fk -> generateForeignKey(fk, sqlCreateTable));
		
		sqlCreateTable.cat(")");
		return sqlCreateTable.toString();
	}
	
	/**
	 * Arranges given foreign keys to aggregates one appearing several times on their name to create a single one with
	 * all columns of initial foreign key. Others (anonymous and appearing only once) are left untouched.
	 * Made to deal with cases where user declares foreign key per column but wants them to create a single one.
	 *
	 * @param uniqueConstraints foreign keys to be arranged
	 * @return a new {@link Set} of arranged foreign keys, in the order of appearance of them
	 */
	protected Set<NewUniqueConstraint> arrangeUniqueConstraints(Set<NewUniqueConstraint> uniqueConstraints) {
		Set<NewUniqueConstraint> effectiveForeignKeys = new KeepOrderSet<>();
		// all unnamed foreign keys are added without special process to final foreign keys list
		// whereas ones with names that appear several times (more than one) are considered to be aggregated
		// and creates a foreign key with aggregated columns.
		Map<String, List<NewUniqueConstraint>> ucPerNonNullName = uniqueConstraints.stream()
				.filter(Predicates.predicate(NewUniqueConstraint::getName, Objects::nonNull))
				.collect(Collectors.groupingBy(NewUniqueConstraint::getName));
		ucPerNonNullName.values().removeIf(list -> list.size() < 2);
		
		List<NewUniqueConstraint> alreadyTreatedMark = new ArrayList<>();
		uniqueConstraints.forEach(uniqueConstraint -> {
			String ucName = uniqueConstraint.getName();
			if (ucName == null) {
				effectiveForeignKeys.add(uniqueConstraint);
			} else {
				List<NewUniqueConstraint> uniqueConstraintsWithSameName = ucPerNonNullName.get(ucName);
				if (uniqueConstraintsWithSameName != null) {
					if (uniqueConstraintsWithSameName != alreadyTreatedMark) {
						NewUniqueConstraint newForeignKey = new NewUniqueConstraint(
								uniqueConstraintsWithSameName.stream().flatMap(uc -> uc.getColumns().stream()).collect(Collectors.toList()));
						newForeignKey.setName(Iterables.first(uniqueConstraintsWithSameName).getName());
						effectiveForeignKeys.add(newForeignKey);
						ucPerNonNullName.put(ucName, alreadyTreatedMark);
					}
				} else {
					effectiveForeignKeys.add(uniqueConstraint);
				}
			}
		});
		return effectiveForeignKeys;
	}
	
	/**
	 * Arranges given foreign keys to aggregates one appearing several times on their name to create a single one with
	 * all columns of initial foreign key. Others (anonymous and appearing only once) are left untouched.
	 * Made to deal with cases where user declares foreign key per column but wants them to create a single one.
	 *
	 * @param foreignKeys foreign keys to be arranged
	 * @return a new {@link Set} of arranged foreign keys, in the order of appearance of them
	 */
	protected Set<NewForeignKey> arrangeForeignKeys(Set<NewForeignKey> foreignKeys) {
		Set<NewForeignKey> effectiveForeignKeys = new KeepOrderSet<>();
		// all unnamed foreign keys are added without special process to final foreign keys list
		// whereas ones with names that appear several times (more than one) are considered to be aggregated
		// and creates a foreign key with aggregated columns.
		Map<String, List<NewForeignKey>> fkPerNonNullName = foreignKeys.stream()
				.filter(Predicates.predicate(NewForeignKey::getName, Objects::nonNull))
				.collect(Collectors.groupingBy(NewForeignKey::getName));
		fkPerNonNullName.values().removeIf(list -> list.size() < 2);
		
		List<NewForeignKey> alreadyTreatedMark = new ArrayList<>();
		foreignKeys.forEach(foreignKey -> {
			String fkName = foreignKey.getName();
			if (fkName == null) {
				effectiveForeignKeys.add(foreignKey);
			} else {
				List<NewForeignKey> foreignKeysWithSameName = fkPerNonNullName.get(fkName);
				if (foreignKeysWithSameName != null) {
					if (foreignKeysWithSameName != alreadyTreatedMark) {
						NewForeignKey newForeignKey = new NewForeignKey(Iterables.first(foreignKeysWithSameName).getReferencedTable());
						newForeignKey.setName(fkName);
						foreignKeysWithSameName.forEach(foreignKeyWithSameName -> {
							Entry<String, String> columns = Iterables.first(foreignKeyWithSameName.getColumnReferences());
							newForeignKey.addColumnReference(columns.getKey(), columns.getValue());
						});
						effectiveForeignKeys.add(newForeignKey);
						fkPerNonNullName.put(fkName, alreadyTreatedMark);
					}
				} else {
					effectiveForeignKeys.add(foreignKey);
				}
			}
		});
		return effectiveForeignKeys;
	}
	
	protected void generateUniqueConstraint(NewUniqueConstraint uniqueConstraint, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(",")
				.catIf(uniqueConstraint.getName() != null, " constraint ", uniqueConstraint.getName())
				.cat(" unique (").ccat(uniqueConstraint.getColumns(), ", ").cat(")");
	}
	
	protected void generateForeignKey(NewForeignKey foreignKey, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(",")
				.catIf(foreignKey.getName() != null, " constraint ", foreignKey.getName())
				.cat(" foreign key (")
				.ccat(foreignKey.getColumnReferences().keySet(), ", ")
				.cat(") references ").cat(foreignKey.getReferencedTable()).cat("(")
				.ccat(foreignKey.getColumnReferences().values(), ", ").cat(")");
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
				.catIf(column.isUnique(), " unique")
				.catIf(column.isAutoIncrement(), " auto_increment")
				.catIf(column.getDefaultValue() != null, " default ", column.getDefaultValue())
		;
	}
	
	
	/**
	 * A {@link StringAppender} that automatically appends {@link NewColumn} names
	 */
	protected class DDLAppender extends StringAppender {
		
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
