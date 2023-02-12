package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.ColumnAlteration;

/**
 * @author Guillaume Mary
 */
public class ColumnAlterationSupport implements ColumnAlteration {

	private final ModifyColumn modifyColumn;

	public ColumnAlterationSupport(String tableName, String columnName, String sqlType) {
		modifyColumn = new ModifyColumn(tableName, columnName, sqlType);
	}

	public ColumnAlterationSupport(String tableName, String columnName, String sqlType, String extraArguments) {
		modifyColumn = new ModifyColumn(tableName, columnName, sqlType, extraArguments);
	}

	@Override
	public ColumnAlteration notNull() {
		modifyColumn.notNull();
		return this;
	}

	@Override
	public ColumnAlteration autoIncrement() {
		modifyColumn.autoIncrement();
		return this;
	}

	@Override
	public ColumnAlteration defaultValue(String defaultValue) {
		modifyColumn.setDefaultValue(defaultValue);
		return this;
	}

	@Override
	public ColumnAlteration setSchema(String schemaName) {
		modifyColumn.setSchemaName(schemaName);
		return this;
	}
	
	@Override
	public ColumnAlteration setCatalog(String catalogName) {
		modifyColumn.setCatalogName(catalogName);
		return this;
	}
	
	@Override
	public ModifyColumn build() {
		return modifyColumn;
	}
}
