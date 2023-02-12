package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.impl.SupportedChange;

/**
 * @author Guillaume Mary
 */
public class AddColumn implements SupportedChange {

	private final Table table;
	private final String name;
	private final String sqlType;
	private final String extraArguments;
	private boolean nullable = true;
	private String defaultValue;
	private boolean autoIncrement = false;

	/**
	 * Creates a statement for column creation
	 *
	 * @param name column name to be created
	 * @param sqlType type of column to be created
	 */
	public AddColumn(String tableName, String name, String sqlType) {
		this(tableName, name, sqlType, null);
	}

	/**
	 * Creates a statement for column creation
	 *
	 * @param name column name to be created
	 * @param sqlType type of column to be created
	 * @param extraArguments extra column argument, like collation or unknown one specific to database vendor,
	 *                       which will come hereafter sql type
	 */
	public AddColumn(String tableName, String name, String sqlType, String extraArguments) {
		this.table = new Table(tableName);
		this.name = name;
		this.sqlType = sqlType;
		this.extraArguments = extraArguments;
	}

	public Table getTable() {
		return table;
	}

	public String getName() {
		return name;
	}

	public String getSqlType() {
		return sqlType;
	}

	public String getExtraArguments() {
		return extraArguments;
	}

	public boolean isNullable() {
		return nullable;
	}

	public AddColumn notNull() {
		setNullable(false);
		return this;
	}

	public AddColumn setNullable(boolean nullable) {
		this.nullable = nullable;
		return this;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public AddColumn setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
		return this;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public AddColumn autoIncrement() {
		setAutoIncrement(true);
		return this;
	}

	public AddColumn setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
		return this;
	}

	public String getSchemaName() {
		return this.table.getSchemaName();
	}
	
	public AddColumn setSchemaName(String schemaName) {
		this.table.setSchemaName(schemaName);
		return this;
	}
	
	public String getCatalogName() {
		return this.table.getCatalogName();
	}
	
	public AddColumn setCatalogName(String catalogName) {
		this.table.setCatalogName(catalogName);
		return this;
	}

	public static class ColumnRenaming {

		private final String oldName;
		private final String newName;

		public ColumnRenaming(String oldName, String newName) {
			this.oldName = oldName;
			this.newName = newName;
		}

		public String getNewName() {
			return newName;
		}

		public String getOldName() {
			return oldName;
		}
	}
}
