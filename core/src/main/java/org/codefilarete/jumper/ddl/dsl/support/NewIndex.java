package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Set;

import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewIndex extends SupportedChange {
	
	private final String name;
	private final Table table;
	
	private boolean unique;
	
	private final Set<String> columns = new KeepOrderSet<>();
	
	public NewIndex(String name, Table table) {
		this.name = name;
		this.table = table;
	}
	
	public String getName() {
		return name;
	}
	
	public Table getTable() {
		return table;
	}
	
	public boolean isUnique() {
		return unique;
	}
	
	public void setUnique() {
		setUnique(true);
	}
	
	public void setUnique(boolean unique) {
		this.unique = unique;
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
