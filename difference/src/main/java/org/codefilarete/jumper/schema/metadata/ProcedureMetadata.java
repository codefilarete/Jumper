package org.codefilarete.jumper.schema.metadata;

import org.codefilarete.jumper.schema.metadata.MetadataElement.SchemaNamespaceElementSupport;

public class ProcedureMetadata extends SchemaNamespaceElementSupport implements MetadataElement {
	
	private final String name;
	private final String remarks;
	private final short type;
	private final String specificName;
	
	public ProcedureMetadata(String catalog, String schema, String name, String remarks, short type, String specificName) {
		super(catalog, schema);
		this.name = name;
		this.remarks = remarks;
		this.type = type;
		this.specificName = specificName;
	}
	
	public String getName() {
		return name;
	}
	
	public String getRemarks() {
		return remarks;
	}
	
	public short getType() {
		return type;
	}
	
	public String getSpecificName() {
		return specificName;
	}
}
