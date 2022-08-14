package org.codefilarete.jumper.ddl.dsl.support;

/**
 * @author Guillaume Mary
 */
public class DropTable implements DDLStatement {
	
	private final Table table;
	
	public DropTable(String name) {
		this.table = new Table(name);
	}
	
	public String getName() {
		return this.table.getName();
	}
	
	public String getSchemaName() {
		return this.table.getSchemaName();
	}
	
	public DropTable setSchemaName(String schemaName) {
		this.table.setSchemaName(schemaName);
		return this;
	}
	
	public String getCatalogName() {
		return this.table.getCatalogName();
	}
	
	public DropTable setCatalogName(String catalogName) {
		this.table.setCatalogName(catalogName);
		return this;
	}
}
