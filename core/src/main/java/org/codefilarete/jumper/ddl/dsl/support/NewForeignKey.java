package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Set;

import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewForeignKey extends SupportedChange {
	
	private final String name;
	private final Table sourceTable;
	private final Set<String> sourceColumns = new KeepOrderSet<>();
	private final Table targetTable;
	private final Set<String> targetColumns = new KeepOrderSet<>();
	
	public NewForeignKey(String name, Table sourceTable, Table targetTable) {
		this.name = name;
		this.sourceTable = sourceTable;
		this.targetTable = targetTable;
	}
	
	public String getName() {
		return name;
	}
	
	public Table getSourceTable() {
		return sourceTable;
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
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public String getSchemaName() {
		return this.sourceTable.getSchemaName();
	}
	
	@Override
	public void setSchemaName(String schemaName) {
		this.sourceTable.setSchemaName(schemaName);
		this.targetTable.setSchemaName(schemaName);
	}
	
	@Override
	public String getCatalogName() {
		return this.sourceTable.getCatalogName();
	}
	
	@Override
	public void setCatalogName(String catalogName) {
		this.sourceTable.setCatalogName(catalogName);
		this.targetTable.setCatalogName(catalogName);
	}
}
