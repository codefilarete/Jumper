package org.codefilarete.jumper.schema.metadata;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.jumper.schema.metadata.MetadataElement.TableNamespaceElementSupport;
import org.codefilarete.tool.Duo;

public class IndexMetadata extends TableNamespaceElementSupport implements MetadataElement {
	
	private String name;
	private short type;
	private boolean unique;
	private String indexQualifier;
	private short ordinalPosition;
	private List<Duo<String, Boolean>> columns = new ArrayList<>();
	private long cardinality;
	private long pages;
	private String filterCondition;
	
	public IndexMetadata(String catalog, String schema, String tableName) {
		super(catalog, schema, tableName);
	}
	
	public String getName() {
		return name;
	}
	
	void setName(String name) {
		this.name = name;
	}
	
	public short getType() {
		return type;
	}
	
	void setType(short type) {
		this.type = type;
	}
	
	public boolean isUnique() {
		return unique;
	}
	
	void setUnique(boolean unique) {
		this.unique = unique;
	}
	
	public String getIndexQualifier() {
		return indexQualifier;
	}
	
	void setIndexQualifier(String indexQualifier) {
		this.indexQualifier = indexQualifier;
	}
	
	public short getOrdinalPosition() {
		return ordinalPosition;
	}
	
	void setOrdinalPosition(short ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}
	
	public List<Duo<String, Boolean>> getColumns() {
		return columns;
	}
	
	void addColumn(String columnName, Boolean ascOrDesc) {
		this.columns.add(new Duo<>(columnName, ascOrDesc));
	}
	
	public long getCardinality() {
		return cardinality;
	}
	
	void setCardinality(long cardinality) {
		this.cardinality = cardinality;
	}
	
	public long getPages() {
		return pages;
	}
	
	void setPages(long pages) {
		this.pages = pages;
	}
	
	public String getFilterCondition() {
		return filterCondition;
	}
	
	void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
	}
}
