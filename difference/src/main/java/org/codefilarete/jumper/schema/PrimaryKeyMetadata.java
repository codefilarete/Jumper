package org.codefilarete.jumper.schema;

import org.codefilarete.jumper.schema.MetadataElement.TableNamespaceElementSupport;
import org.codefilarete.tool.collection.KeepOrderSet;

public class PrimaryKeyMetadata extends TableNamespaceElementSupport implements MetadataElement {
	
	private final String name;
	private final KeepOrderSet<String> columns = new KeepOrderSet<>();
	
	public PrimaryKeyMetadata(String catalog, String schema, String tableName,
							  String name) {
		super(catalog, schema, tableName);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	void addColumn(String columnName) {
		this.columns.add(columnName);
	}
	
	public KeepOrderSet<String> getColumns() {
		return this.columns;
	}
	
	@Override
	public String toString() {
		return "PrimaryKey{" +
				"name='" + name + '\'' +
				", sourceTable=" + getTableName() +
				", columns=" + columns.getSurrogate() +
				'}';
	}
}
