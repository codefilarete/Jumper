package org.gama.jumper.ddl.dsl.support;

import org.gama.jumper.ddl.dsl.ForeignKeyCreation;

/**
 * @author Guillaume Mary
 */
public class ForeignKeyCreationSupport implements ForeignKeyCreation {
	
	private final NewForeignKey foreignKey;
	
	public ForeignKeyCreationSupport(String name, String tableName) {
		this.foreignKey = new NewForeignKey(name, new Table(tableName));
	}
	
	@Override
	public ForeignKeyPostSourceColumnOptions addSourceColumn(String name) {
		foreignKey.addSourceColumn(name);
		return new ForeignKeyPostSourceColumnOptions() {
			@Override
			public ForeignKeyPostSourceColumnOptions addSourceColumn(String name) {
				foreignKey.addSourceColumn(name);
				return this;
			}
			
			@Override
			public ForeignKeyPostTargetTableOptions targetTable(String name) {
				foreignKey.setTargetTable(new Table(name));
				return new ForeignKeyPostTargetTableOptions() {
					@Override
					public ForeignKeyPostTargetColumnOptions addTargetColumn(String name) {
						foreignKey.addTargetColumn(name);
						return new ForeignKeyPostTargetColumnOptions() {
							@Override
							public ForeignKeyPostTargetColumnOptions addTargetColumn(String name) {
								foreignKey.addTargetColumn(name);
								return this;
							}
							
							@Override
							public NewForeignKey build() {
								return foreignKey;
							}
						};
					}
				};
			}
		};
//		return new MethodReferenceDispatcher()
//				.redirect(ForeignKeyPostSourceColumnOptions::targetTable, (String s) -> foreignKey.setTargetTable(new Table(s)))
//				.build(ForeignKeyPostSourceColumnOptions.class);
	}
	
	@Override
	public ForeignKeyCreation setSchema(String schemaName) {
		foreignKey.getTable().setSchemaName(schemaName);
		return this;
	}
	
	@Override
	public ForeignKeyCreation setCatalog(String catalogName) {
		foreignKey.getTable().setCatalogName(catalogName);
		return this;
	}
}
