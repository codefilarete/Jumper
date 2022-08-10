package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.IndexCreation;

/**
 * @author Guillaume Mary
 */
public class IndexCreationSupport implements IndexCreation {
	
	private final NewIndex index;
	
	public IndexCreationSupport(String name, String tableName) {
		this.index = new NewIndex(name, new Table(tableName));
	}
	
	@Override
	public IndexCreation addColumn(String name) {
		index.addColumn(name);
		return this;
	}
	
	@Override
	public IndexCreation unique() {
		index.setUnique();
		return this;
	}
	
	@Override
	public IndexCreation setSchema(String schema) {
		index.getTable().setSchemaName(schema);
		return this;
	}
	
	@Override
	public IndexCreation setCatalog(String schema) {
		index.getTable().setCatalogName(schema);
		return this;
	}
	
	@Override
	public NewIndex build() {
		return index;
	}
}
