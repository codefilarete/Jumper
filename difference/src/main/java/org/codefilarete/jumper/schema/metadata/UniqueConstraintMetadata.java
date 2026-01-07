package org.codefilarete.jumper.schema.metadata;

import org.codefilarete.jumper.schema.metadata.MetadataElement.TableNamespaceElementSupport;
import org.codefilarete.tool.collection.KeepOrderSet;

public class UniqueConstraintMetadata extends TableNamespaceElementSupport implements MetadataElement {
	
	private String name;
	private KeepOrderSet<String> columns = new KeepOrderSet<>();
	
	public UniqueConstraintMetadata(String catalog, String schema, String tableName) {
		super(catalog, schema, tableName);
	}
	
	public String getName() {
		return name;
	}
	
	void setName(String name) {
		this.name = name;
	}
	
	public KeepOrderSet<String> getColumns() {
		return columns;
	}
	
	public void setColumns(KeepOrderSet<String> columns) {
		this.columns = columns;
	}
	
	public void addColumn(String name) {
		this.columns.add(name);
	}
}
