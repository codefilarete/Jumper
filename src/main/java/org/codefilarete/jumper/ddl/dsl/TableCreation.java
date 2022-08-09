package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
public interface TableCreation {
	
	TableCreationColumnOption addColumn(String name, String sqlType);
	
	TableCreation setSchema(String schemaName);
	
	TableCreation setCatalog(String catalogName);
	
	NewTable build();
	
	interface TableCreationColumnOption extends TableCreation, ColumnOption {
		
		@Override
		TableCreationColumnOption notNull();
		
		@Override
		TableCreationColumnOption autoIncrement();
		
		@Override
		TableCreationColumnOption defaultValue(String defaultValue);
		
		TableCreationColumnOption inPrimaryKey();

		@Override
		TableCreationColumnOption uniqueConstraint(String name);
	}
}
