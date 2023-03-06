package org.codefilarete.jumper.schema.metadata;

import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.KeepOrderSet;

public class ForeignKeyMetadata implements MetadataElement {
	
	private final String name;
	private final TableNamespaceElement sourceTable;
	private final TableNamespaceElement targetTable;
	private final Set<Duo<String, String>> columns = new KeepOrderSet<>();
	
	public ForeignKeyMetadata(String name,
							  String sourceCatalog, String sourceSchema, String sourceTableName,
							  String targetCatalog, String targetSchema, String targetTableName) {
		this.name = name;
		this.sourceTable = new TableNamespaceElementSupport(sourceCatalog, sourceSchema, sourceTableName);
		this.targetTable = new TableNamespaceElementSupport(targetCatalog, targetSchema, targetTableName);
	}
	
	public String getName() {
		return name;
	}
	
	public TableNamespaceElement getSourceTable() {
		return sourceTable;
	}
	
	public TableNamespaceElement getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Gives all pairs of column names associated by this foreign key : left side is source, right side is referenced
	 *
	 * @return pairs of columns, from source to target, in order defined by this foreign key
	 */
	public Set<Duo<String, String>> getColumns() {
		return columns;
	}
	
	ForeignKeyMetadata addColumn(String sourceColumnName, String targetColumnName) {
		this.columns.add(new Duo<>(sourceColumnName, targetColumnName));
		return this;
	}
	
	@Override
	public String toString() {
		return "ForeignKey{" +
				"name='" + name + '\'' +
				", sourceTable=" + sourceTable.getTableName() +
				", targetTable=" + targetTable.getTableName() +
				", columns=" + columns.stream().map(duo -> duo.getLeft() + " = " + duo.getRight()).collect(Collectors.toList()) +
				'}';
	}
}
