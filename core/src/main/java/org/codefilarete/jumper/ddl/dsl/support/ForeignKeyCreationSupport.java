package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.ForeignKeyCreation;

/**
 * @author Guillaume Mary
 */
public class ForeignKeyCreationSupport extends AbstractSupportedChangeSupport<NewForeignKey, ForeignKeyCreation> implements ForeignKeyCreation {
	
	private final NewForeignKey foreignKey;
	
	public ForeignKeyCreationSupport(String name, String sourceTableName, String targetTableName) {
		this.foreignKey = new NewForeignKey(name, new Table(sourceTableName), new Table(targetTableName));
	}
	
	@Override
	public ForeignKeyCreation addColumnReference(String sourceColumnName, String targetColumnName) {
		foreignKey.addSourceColumn(sourceColumnName);
		foreignKey.addTargetColumn(targetColumnName);
		return this;
	}
	
	@Override
	public ForeignKeyCreation setSchema(String schemaName) {
		foreignKey.setSchemaName(schemaName);
		return this;
	}
	
	@Override
	public ForeignKeyCreation setCatalog(String catalogName) {
		foreignKey.setCatalogName(catalogName);
		return this;
	}
	
	@Override
	public NewForeignKey build() {
		return this.foreignKey;
	}
}
