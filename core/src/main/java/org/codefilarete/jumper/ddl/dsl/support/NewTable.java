package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Arrays;
import java.util.Set;

import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewTable implements SupportedChange {
	
	private final Table table;
	private final Set<NewColumn> columns = new KeepOrderSet<>();
	private NewPrimaryKey primaryKey;
	private final Set<NewUniqueConstraint> uniqueConstraints = new KeepOrderSet<>();
	
	public NewTable(String name) {
		this.table = new Table(name);
	}
	
	public String getName() {
		return this.table.getName();
	}
	
	public String getSchemaName() {
		return this.table.getSchemaName();
	}
	
	public NewTable setSchemaName(String schemaName) {
		this.table.setSchemaName(schemaName);
		return this;
	}
	
	public String getCatalogName() {
		return this.table.getCatalogName();
	}
	
	public NewTable setCatalogName(String catalogName) {
		this.table.setCatalogName(catalogName);
		return this;
	}
	
	public Set<NewColumn> getColumns() {
		return columns;
	}
	
	public void addColumn(NewColumn newColumn) {
		this.columns.add(newColumn);
	}
	
	public void setPrimaryKey(String columnName, String... extraColumnNames) {
		this.primaryKey = new NewPrimaryKey(columnName, extraColumnNames);
	}
	
	public NewPrimaryKey getPrimaryKey() {
		return primaryKey;
	}
	
	public void addUniqueConstraint(String constraintName, String columnName, String... extraColumnNames) {
		this.uniqueConstraints.add(new NewUniqueConstraint(constraintName, columnName, extraColumnNames));
	}
	
	public Set<NewUniqueConstraint> getUniqueConstraints() {
		return uniqueConstraints;
	}
	
	public static class NewPrimaryKey {
		
		private final KeepOrderSet<String> columns = new KeepOrderSet<>();
		
		public NewPrimaryKey(String columnName, String... extraColumnNames) {
			this.columns.add(columnName);
			this.columns.addAll(Arrays.asList(extraColumnNames));
		}
		
		public KeepOrderSet<String> getColumns() {
			return columns;
		}
	}
	
	public static class NewColumn {
		
		private final String name;
		private final String sqlType;
		private final String extraArguments;
		private boolean nullable = true;
		private String defaultValue;
		private boolean autoIncrement = false;
		private String uniqueConstraintName;
		
		/**
		 * Creates a statement for column creation
		 *
		 * @param name column name to be created
		 * @param sqlType type of column to be created
		 */
		public NewColumn(String name, String sqlType) {
			this(name, sqlType, null);
		}
		
		/**
		 * Creates a statement for column creation
		 *
		 * @param name column name to be created
		 * @param sqlType type of column to be created
		 * @param extraArguments extra column creation argument, like collation or unknown one specific to database vendor,
		 * which will come hereafter sql type
		 */
		public NewColumn(String name, String sqlType, String extraArguments) {
			this.name = name;
			this.sqlType = sqlType;
			this.extraArguments = extraArguments;
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
		
		public NewColumn notNull() {
			setNullable(false);
			return this;
		}
		
		public NewColumn setNullable(boolean nullable) {
			this.nullable = nullable;
			return this;
		}
		
		public String getDefaultValue() {
			return defaultValue;
		}
		
		public NewColumn setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
			return this;
		}
		
		public boolean isAutoIncrement() {
			return autoIncrement;
		}
		
		public NewColumn autoIncrement() {
			setAutoIncrement(true);
			return this;
		}
		
		public NewColumn setAutoIncrement(boolean autoIncrement) {
			this.autoIncrement = autoIncrement;
			return this;
		}
		
		public String getUniqueConstraintName() {
			return uniqueConstraintName;
		}
		
		public void setUniqueConstraint(String name) {
			this.uniqueConstraintName = name;
		}
	}
	
	public static class NewUniqueConstraint {
		
		private final String name;
		
		private final KeepOrderSet<String> columns = new KeepOrderSet<>();
		
		public NewUniqueConstraint(String name, String columnName, String... extraColumnNames) {
			this.name = name;
			this.columns.add(columnName);
			this.columns.addAll(Arrays.asList(extraColumnNames));
		}
		
		public String getName() {
			return name;
		}
		
		public KeepOrderSet<String> getColumns() {
			return columns;
		}
	}
}
