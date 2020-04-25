package org.gama.jumper.ddl.dsl.support;

import java.util.Set;

import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewTable implements StructureDefinition {
	
	private final String name;
	private String schemaName;
	private String catalogName;
	private final Set<NewColumn> columns = new KeepOrderSet<>();
	private NewPrimaryKey primaryKey;
	
	public NewTable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
	
	public NewTable setSchemaName(String schemaName) {
		this.schemaName = schemaName;
		return this;
	}
	
	public String getCatalogName() {
		return catalogName;
	}
	
	public NewTable setCatalogName(String catalogName) {
		this.catalogName = catalogName;
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
	
	public void addPrimaryKeyColumn(NewColumn column) {
		if (this.primaryKey == null) {
			this.primaryKey = new NewPrimaryKey();
		}
		this.primaryKey.addColumn(column);
	}
	
	public NewPrimaryKey getPrimaryKey() {
		return primaryKey;
	}
}
