package org.codefilarete.jumper.schema;

import org.codefilarete.jumper.schema.MetadataElement.SchemaNamespaceElementSupport;

public class ViewMetadata extends SchemaNamespaceElementSupport implements MetadataElement {
	
	private String name;
	
	public ViewMetadata(String catalog, String schema) {
		super(catalog, schema);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
