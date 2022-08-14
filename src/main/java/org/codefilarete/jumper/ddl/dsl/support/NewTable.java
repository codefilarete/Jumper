package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Arrays;
import java.util.Set;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewTable implements DDLStatement {
	
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
		if (Iterables.contains(columns, NewColumn::getName, newColumn.getName()::equals)) {
			throw new DuplicateColumnDefinition(newColumn);
		}
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
	
	public static class NewPrimaryKey implements DDLStatement {
		
		private final KeepOrderSet<String> columns = new KeepOrderSet<>();
		
		public NewPrimaryKey(String columnName, String... extraColumnNames) {
			this.columns.add(columnName);
			this.columns.addAll(Arrays.asList(extraColumnNames));
		}
		
		public KeepOrderSet<String> getColumns() {
			return columns;
		}
	}
	
	public static class NewColumn implements DDLStatement {
		
		private final String name;
		private final String sqlType;
		private boolean nullable = true;
		private String defaultValue;
		private boolean autoIncrement = false;
		private String uniqueConstraintName;
		
		public NewColumn(String name, String sqlType) {
			this.name = name;
			this.sqlType = sqlType;
		}
		
		public String getName() {
			return name;
		}
		
		public String getSqlType() {
			return sqlType;
		}
		
		public boolean isNullable() {
			return nullable;
		}
		
		public void notNull() {
			setNullable(false);
		}
		
		public void setNullable(boolean nullable) {
			this.nullable = nullable;
		}
		
		public String getDefaultValue() {
			return defaultValue;
		}
		
		public void setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
		}
		
		public boolean isAutoIncrement() {
			return autoIncrement;
		}
		
		public void autoIncrement() {
			setAutoIncrement(true);
		}
		
		public void setAutoIncrement(boolean autoIncrement) {
			this.autoIncrement = autoIncrement;
		}
		
		public String getUniqueConstraintName() {
			return uniqueConstraintName;
		}
		
		public void setUniqueConstraint(String name) {
			this.uniqueConstraintName = name;
		}
	}
	
	public static class NewUniqueConstraint implements DDLStatement {
		
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
