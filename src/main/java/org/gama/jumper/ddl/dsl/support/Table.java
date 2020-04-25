package org.gama.jumper.ddl.dsl.support;

/**
 * @author Guillaume Mary
 */
public class Table {
	
	private final String name;
	private String schemaName;
	private String catalogName;
	
	public Table(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
	
	public Table setSchemaName(String schemaName) {
		this.schemaName = schemaName;
		return this;
	}
	
	public String getCatalogName() {
		return catalogName;
	}
	
	public Table setCatalogName(String catalogName) {
		this.catalogName = catalogName;
		return this;
	}
}
