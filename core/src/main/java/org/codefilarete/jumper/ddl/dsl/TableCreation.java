package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * Contract for {@link NewTable} creation through fluent API
 *
 * @author Guillaume Mary
 */
public interface TableCreation extends FluentSupportedChange<NewTable, TableCreation> {
	
	TableCreationColumnOption addColumn(String name, String sqlType);
	
	TableCreation setPrimaryKey(String columnName, String... extraColumnNames);
	
	UniqueConstraintInTableCreation addUniqueConstraint(String columnName, String... extraColumnNames);
	
	ForeignKeyInTableCreation addForeignKey(String targetTableName);
	
	// Can't inherit from ForeignKeyCreation because of conflict with FluentSupportedChange, which makes sens since
	// this interface hasn't the same goal as FluentSupportedChange
	interface ForeignKeyInTableCreation extends TableCreation {
		
		ForeignKeyInTableCreation addColumnReference(String sourceColumnName, String targetColumnName);
		
		ForeignKeyInTableCreation setForeignKeyName(String foreignKeyName);
	}
	
	interface UniqueConstraintInTableCreation extends TableCreation {
		
		UniqueConstraintInTableCreation setUniqueConstraintName(String uniqueConstraintName);
	}
	
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
		TableCreationColumnOption unique();
		
		@Override
		TableCreationColumnOption unique(String uniqueConstraintName);
		
		@Override
		TableCreationColumnOption references(String tableName, String columnName);
		
		@Override
		TableCreationColumnOption references(String tableName, String columnName, String foreignKeyName);
	}
}
