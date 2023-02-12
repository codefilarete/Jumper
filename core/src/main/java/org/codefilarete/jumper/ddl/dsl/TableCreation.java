package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
public interface TableCreation extends ChangeSet.ChangeBuilder {
	
	TableCreationColumnOption addColumn(String name, String sqlType);
	
	TableCreation setSchema(String schemaName);
	
	TableCreation setCatalog(String catalogName);
	
	TableCreation primaryKey(String columnName, String... extraColumnNames);
	
	TableCreation uniqueConstraint(String constraintName, String columnName, String... extraColumnNames);
	
	NewTable build();
	
	interface TableCreationColumnOption extends TableCreation, ColumnOption {
		
		@Override
		TableCreationColumnOption notNull();
		
		@Override
		TableCreationColumnOption autoIncrement();
		
		@Override
		TableCreationColumnOption defaultValue(String defaultValue);
		
		@Override
		TableCreationColumnOption primaryKey();
		
		@Override
		TableCreationColumnOption uniqueConstraint(String name);
	}
}
