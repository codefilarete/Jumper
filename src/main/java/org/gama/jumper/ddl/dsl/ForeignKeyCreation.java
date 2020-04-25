package org.gama.jumper.ddl.dsl;

import org.gama.jumper.ddl.dsl.support.NewForeignKey;

/**
 * @author Guillaume Mary
 */
public interface ForeignKeyCreation {
	
	ForeignKeyPostSourceColumnOptions addSourceColumn(String name);
	
	ForeignKeyCreation setSchema(String schemaName);
	
	ForeignKeyCreation setCatalog(String catalogName);
	
	interface ForeignKeyPostSourceColumnOptions {
		
		ForeignKeyPostSourceColumnOptions addSourceColumn(String name);
		
		ForeignKeyPostTargetTableOptions targetTable(String name);
	}
	
	interface ForeignKeyPostTargetTableOptions {
		
		ForeignKeyPostTargetColumnOptions addTargetColumn(String name);
	}
	
	interface ForeignKeyPostTargetColumnOptions {
		
		ForeignKeyPostTargetColumnOptions addTargetColumn(String name);
		
		NewForeignKey build();
	}
}
