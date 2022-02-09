package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Set;

import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewIndex {
	
	private final String name;
	private final Table table;
	
	private boolean unique;
	
	private final Set<Column> columns = new KeepOrderSet<>();
	
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
	
	public Set<Column> getColumns() {
		return columns;
	}
	
	public void addColumn(Column column) {
		this.columns.add(column);
	}
}
