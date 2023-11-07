package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.impl.SupportedChange;

/**
 * @author Guillaume Mary
 */
public class DropTable extends SupportedChange {
	
	private final Table table;
	
	public DropTable(String name) {
		this.table = new Table(name);
	}
	
	public String getName() {
		return this.table.getName();
	}
	
	@Override
	public String getSchemaName() {
		return this.table.getSchemaName();
	}
	
	@Override
	public void setSchemaName(String schemaName) {
		this.table.setSchemaName(schemaName);
	}
	
	@Override
	public String getCatalogName() {
		return this.table.getCatalogName();
	}
	
	@Override
	public void setCatalogName(String catalogName) {
		this.table.setCatalogName(catalogName);
	}
}
