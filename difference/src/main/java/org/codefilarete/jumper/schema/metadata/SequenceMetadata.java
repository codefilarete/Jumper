package org.codefilarete.jumper.schema.metadata;

import org.codefilarete.jumper.schema.metadata.MetadataElement.SchemaNamespaceElementSupport;

public class SequenceMetadata extends SchemaNamespaceElementSupport implements MetadataElement {
	
	private final String name;
	
	public SequenceMetadata(String catalog, String schema, String name) {
		super(catalog, schema);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
