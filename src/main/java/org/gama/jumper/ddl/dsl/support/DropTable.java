package org.gama.jumper.ddl.dsl.support;

/**
 * @author Guillaume Mary
 */
public class DropTable {
	
	private final String name;
	private String schemaName;
	private String catalogName;
	
	public DropTable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
	
	public DropTable setSchemaName(String schemaName) {
		this.schemaName = schemaName;
		return this;
	}
	
	public String getCatalogName() {
		return catalogName;
	}
	
	public DropTable setCatalogName(String catalogName) {
		this.catalogName = catalogName;
		return this;
	}
}
