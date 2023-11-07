package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * Contract for {@link NewTable} creation through fluent API
 *
 * @author Guillaume Mary
 */
public interface TableCreation extends FluentSupportedChange<NewTable, TableCreation> {
	
	TableCreationColumnOption addColumn(String name, String sqlType);
	
	TableCreation primaryKey(String columnName, String... extraColumnNames);
	
	TableCreation uniqueConstraint(String constraintName, String columnName, String... extraColumnNames);
	
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
