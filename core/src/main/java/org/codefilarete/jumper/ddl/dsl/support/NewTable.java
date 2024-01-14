package org.codefilarete.jumper.ddl.dsl.support;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;

import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewTable extends SupportedChange {
	
	private final Table table;
	private final Set<NewColumn> columns = new KeepOrderSet<>();
	private NewPrimaryKey primaryKey;
	private final Set<NewUniqueConstraint> uniqueConstraints = new KeepOrderSet<>();
	private final Set<NewForeignKey> foreignKeys = new KeepOrderSet<>();
	
	public NewTable(String name) {
		this.table = new Table(name);
	}
	
	public String getName() {
		return this.table.getName();
	}
	
	@Override
	public String getSchemaName() {
		return this.table.getSchemaName();
	}
	
	@Override
	public void setSchemaName(String schemaName) {
		this.table.setSchemaName(schemaName);
	}
	
	@Override
	public String getCatalogName() {
		return this.table.getCatalogName();
	}
	
	@Override
	public void setCatalogName(String catalogName) {
		this.table.setCatalogName(catalogName);
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
	
	public NewUniqueConstraint addUniqueConstraint(String columnName, String... extraColumnNames) {
		NewUniqueConstraint uniqueConstraint = new NewUniqueConstraint(columnName, extraColumnNames);
		this.uniqueConstraints.add(uniqueConstraint);
		return uniqueConstraint;
	}
	
	public NewForeignKey addForeignKey(String targetTableName) {
		NewForeignKey foreignKeyCreationSupport = new NewForeignKey(targetTableName);
		this.foreignKeys.add(foreignKeyCreationSupport);
		return foreignKeyCreationSupport;
	}
	
	public Set<NewUniqueConstraint> getUniqueConstraints() {
		return uniqueConstraints;
	}
	
	public Set<NewForeignKey> getForeignKeys() {
		return foreignKeys;
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
		private boolean unique = false;
		
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
		
		public NewColumn unique() {
			return setUnique(true);
		}
		
		public NewColumn setUnique(boolean unique) {
			this.unique = unique;
			return this;
		}
		
		public boolean isUnique() {
			return unique;
		}
	}
	
	public static class NewUniqueConstraint {
		
		private String name;
		
		private final KeepOrderSet<String> columns = new KeepOrderSet<>();
		
		public NewUniqueConstraint(Iterable<String> columnNames) {
			Iterables.copy(columnNames, columns);
		}
		
		public NewUniqueConstraint(String columnName, String... extraColumnNames) {
			this.columns.add(columnName);
			this.columns.addAll(Arrays.asList(extraColumnNames));
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public KeepOrderSet<String> getColumns() {
			return columns;
		}
	}
	
	/**
	 * Represents a foreign key declared directly during table creation.
	 * Some Database vendor such as SQLite doesn't support foreign key creation out of table creation
	 * (https://stackoverflow.com/a/1884841, https://www.sqlite.org/omitted.html), so in such case one can't use
	 * {@link org.codefilarete.jumper.ddl.dsl.support.NewForeignKey}
	 *
	 * @author Guillaume Mary
	 */
	public static class NewForeignKey {
		
		private final String referencedTable;
		
		@Nullable
		private String name;
		
		private final KeepOrderMap<String, String> columnReferences = new KeepOrderMap<>();
		
		public NewForeignKey(String referencedTable) {
			this.referencedTable = referencedTable;
		}
		
		public String getReferencedTable() {
			return referencedTable;
		}
		
		@Nullable
		public String getName() {
			return name;
		}
		
		public NewForeignKey setName(@Nullable String name) {
			this.name = name;
			return this;
		}
		
		public NewForeignKey addColumnReference(String sourceColumn, String referencedColumn) {
			this.columnReferences.put(sourceColumn, referencedColumn);
			return this;
		}
		
		public KeepOrderMap<String, String> getColumnReferences() {
			return columnReferences;
		}
	}
}