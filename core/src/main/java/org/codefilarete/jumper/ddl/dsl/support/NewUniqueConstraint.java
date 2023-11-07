package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Set;

import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.collection.KeepOrderSet;

public class NewUniqueConstraint extends SupportedChange {
	
	private final String name;
	private final Table table;
	
	private final Set<String> columns = new KeepOrderSet<>();
	
	public NewUniqueConstraint(String name, Table table) {
		this.name = name;
		this.table = table;
	}
	
	public String getName() {
		return name;
	}
	
	public Table getTable() {
		return table;
	}
	
	public Set<String> getColumns() {
		return columns;
	}
	
	public void addColumn(String columnName) {
		this.columns.add(columnName);
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
}
