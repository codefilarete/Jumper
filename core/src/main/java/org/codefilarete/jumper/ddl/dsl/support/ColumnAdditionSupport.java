package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.ColumnAddition;

/**
 * @author Guillaume Mary
 */
public class ColumnAdditionSupport extends AbstractSupportedChangeSupport<AddColumn, ColumnAddition> implements ColumnAddition {
	
	private final AddColumn addColumn;
	
	public ColumnAdditionSupport(String tableName, String columnName, String sqlType) {
		addColumn = new AddColumn(tableName, columnName, sqlType);
	}
	
	public ColumnAdditionSupport(String tableName, String columnName, String sqlType, String extraArguments) {
		addColumn = new AddColumn(tableName, columnName, sqlType, extraArguments);
	}
	
	@Override
	public ColumnAddition notNull() {
		addColumn.notNull();
		return this;
	}
	
	@Override
	public ColumnAddition autoIncrement() {
		addColumn.autoIncrement();
		return this;
	}
	
	@Override
	public ColumnAddition defaultValue(String defaultValue) {
		addColumn.setDefaultValue(defaultValue);
		return this;
	}
	
	@Override
	public ColumnAddition setSchema(String schemaName) {
		this.addColumn.setSchemaName(schemaName);
		return this;
	}
	
	@Override
	public ColumnAddition setCatalog(String catalogName) {
		this.addColumn.setCatalogName(catalogName);
		return this;
	}
	
	@Override
	public AddColumn build() {
		return addColumn;
	}
}
