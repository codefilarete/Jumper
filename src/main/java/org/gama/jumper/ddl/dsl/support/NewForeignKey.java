package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Set;

import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewForeignKey {
	
	private final String name;
	private final Table table;
	private final Set<String> sourceColumns = new KeepOrderSet<>();
	private Table targetTable;
	private final Set<String> targetColumns = new KeepOrderSet<>();
	
	public NewForeignKey(String name, Table table) {
		this.name = name;
		this.table = table;
	}
	
	public String getName() {
		return name;
	}
	
	public Table getTable() {
		return table;
	}
	
	public Set<String> getSourceColumns() {
		return sourceColumns;
	}
	
	public void addSourceColumn(String name) {
		sourceColumns.add(name);
	}
	
	public Set<String> getTargetColumns() {
		return targetColumns;
	}
	
	public void addTargetColumn(String name) {
		targetColumns.add(name);
	}
	
	public void setTargetTable(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
}
