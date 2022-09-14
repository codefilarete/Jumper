package org.codefilarete.jumper.schema;

import org.codefilarete.jumper.schema.MetadataElement.SchemaNamespaceElementSupport;

public class TableMetadata extends SchemaNamespaceElementSupport implements MetadataElement {
	
	private String name;
	private String type;
	private String remarks;
	private String typeCatalog;
	private String typeSchema;
	private String typeName;
	private String selfReferencingColName;
	private String refGeneration;
	
	public TableMetadata(String catalog, String schema) {
		super(catalog, schema);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public String getRemarks() {
		return remarks;
	}
	
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	
	public String getTypeCatalog() {
		return typeCatalog;
	}
	
	public void setTypeCatalog(String typeCatalog) {
		this.typeCatalog = typeCatalog;
	}
	
	public String getTypeSchema() {
		return typeSchema;
	}
	
	public void setTypeSchema(String typeSchema) {
		this.typeSchema = typeSchema;
	}
	
	public String getTypeName() {
		return typeName;
	}
	
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
	public String getSelfReferencingColName() {
		return selfReferencingColName;
	}
	
	public void setSelfReferencingColName(String selfReferencingColName) {
		this.selfReferencingColName = selfReferencingColName;
	}
	
	public String getRefGeneration() {
		return refGeneration;
	}
	
	public void setRefGeneration(String refGeneration) {
		this.refGeneration = refGeneration;
	}
}
