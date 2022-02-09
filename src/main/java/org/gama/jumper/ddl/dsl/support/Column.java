package org.codefilarete.jumper.ddl.dsl.support;

/**
 * @author Guillaume Mary
 */
public class Column {
	
	private final String name;
	private String tableName;
	private String schemaName;
	private String catalogName;
	
	public Column(String name) {
		this.name = name;
	}
	
	public Column(String name, String tableName) {
		this.name = name;
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String getName() {
		return name;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
	
	public Column setSchemaName(String schemaName) {
		this.schemaName = schemaName;
		return this;
	}
	
	public String getCatalogName() {
		return catalogName;
	}
	
	public Column setCatalogName(String catalogName) {
		this.catalogName = catalogName;
		return this;
	}
}
