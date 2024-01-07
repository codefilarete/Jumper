package org.codefilarete.jumper.schema;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.metadata.SchemaMetadataReader;

public abstract class SchemaElementCollector {
	
	protected final SchemaMetadataReader metadataReader;
	protected String catalog;
	protected String schema;
	protected String tableNamePattern;
	
	/**
	 * Generic constructor. Will create {@link Schema} elements from given {@link SchemaMetadataReader} on {@link #collect()}
	 * call.
	 *
	 * @param metadataReader
	 */
	public SchemaElementCollector(SchemaMetadataReader metadataReader) {
		this.metadataReader = metadataReader;
	}
	
	public SchemaElementCollector withCatalog(String catalog) {
		this.catalog = catalog;
		return this;
	}
	
	public SchemaElementCollector withSchema(String schema) {
		this.schema = schema;
		return this;
	}
	
	public SchemaElementCollector withTableNamePattern(String tableNamePattern) {
		this.tableNamePattern = tableNamePattern;
		return this;
	}
	
	public abstract Schema collect();
	
}
