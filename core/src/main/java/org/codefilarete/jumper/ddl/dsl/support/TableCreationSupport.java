package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.ColumnOption;
import org.codefilarete.jumper.ddl.dsl.TableCreation;
import org.codefilarete.reflection.MethodReferenceDispatcher;

/**
 * @author Guillaume Mary
 */
public class TableCreationSupport extends AbstractSupportedChangeSupport<NewTable, TableCreation> implements TableCreation {
	
	private final NewTable table;
	
	public TableCreationSupport(String name) {
		table = new NewTable(name);
	}
	
	@Override
	public TableCreationColumnOption addColumn(String name, String sqlType) {
		NewTable.NewColumn newColumn = new NewTable.NewColumn(name, sqlType);
		this.table.addColumn(newColumn);
		return new MethodReferenceDispatcher()
				.redirect(ColumnOption.class, new ColumnOption() {
					@Override
					public ColumnOption notNull() {
						newColumn.notNull();
						return null;
					}
					
					@Override
					public ColumnOption autoIncrement() {
						newColumn.setAutoIncrement(true);
						return null;
					}
					
					@Override
					public ColumnOption defaultValue(String defaultValue) {
						newColumn.setDefaultValue(defaultValue);
						return null;
					}
					
					@Override
					public ColumnOption primaryKey() {
						table.setPrimaryKey(newColumn.getName());
						return null;
					}
					
					@Override
					public TableCreationColumnOption uniqueConstraint(String name) {
						newColumn.setUniqueConstraint(name);
						return null;
					}
				}, true)
				.fallbackOn(this)
				.build(TableCreationColumnOption.class);
	}
	
	@Override
	public TableCreation setSchema(String schemaName) {
		table.setSchemaName(schemaName);
		return this;
	}
	
	@Override
	public TableCreation setCatalog(String catalogName) {
		table.setCatalogName(catalogName);
		return this;
	}
	
	@Override
	public TableCreation primaryKey(String columnName, String... extraColumnNames) {
		table.setPrimaryKey(columnName, extraColumnNames);
		return this;
	}
	
	@Override
	public TableCreation uniqueConstraint(String constraintName, String columnName, String... extraColumnNames) {
		table.addUniqueConstraint(constraintName, columnName, extraColumnNames);
		return this;
	}
	
	@Override
	public NewTable build() {
		return table;
	}
}
