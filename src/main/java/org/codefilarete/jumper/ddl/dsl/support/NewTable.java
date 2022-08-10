package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Set;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewTable implements StructureDefinition {
	
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
	
	public void setPrimaryKey(NewPrimaryKey primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public NewPrimaryKey getPrimaryKey() {
		return primaryKey;
	}
	
	public void addUniqueConstraint(NewUniqueConstraint constraint) {
		this.uniqueConstraints.add(constraint);
	}
	
	public Set<NewUniqueConstraint> getUniqueConstraints() {
		return uniqueConstraints;
	}
}
