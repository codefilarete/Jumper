package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.ColumnOption;
import org.codefilarete.jumper.ddl.dsl.TableCreation;
import org.codefilarete.reflection.MethodReferenceDispatcher;

/**
 * @author Guillaume Mary
 */
public class TableCreationSupport extends AbstractSupportedChangeSupport<NewTable, TableCreation> implements TableCreation {
	
	private final NewTable table;
	
	public TableCreationSupport(String name) {
		table = new NewTable(name);
	}
	
	@Override
	public TableCreationColumnOption addColumn(String name, String sqlType) {
		NewTable.NewColumn newColumn = new NewTable.NewColumn(name, sqlType);
		this.table.addColumn(newColumn);
		return new MethodReferenceDispatcher()
				.redirect(ColumnOption.class, new ColumnOption() {
					@Override
					public ColumnOption notNull() {
						newColumn.notNull();
						return null;
					}
					
					@Override
					public ColumnOption autoIncrement() {
						newColumn.setAutoIncrement(true);
						return null;
					}
					
					@Override
					public ColumnOption defaultValue(String defaultValue) {
						newColumn.setDefaultValue(defaultValue);
						return null;
					}
					
					@Override
					public ColumnOption primaryKey() {
						table.setPrimaryKey(newColumn.getName());
						return null;
					}
					
					@Override
					public ColumnOption unique() {
						newColumn.unique();
						return null;
					}
					
					@Override
					public ColumnOption unique(String uniqueConstraintName) {
						NewTable.NewUniqueConstraint uniqueConstraint = table.addUniqueConstraint(newColumn.getName());
						uniqueConstraint.setName(uniqueConstraintName);
						return null;
					}
					
					@Override
					public ColumnOption references(String tableName, String columnName) {
						// we don't do anything here because it is overwritten below by appropriate redirection to
						// handle extra options that adds column to the
						return null;
					}
					
					@Override
					public ColumnOption references(String tableName, String columnName, String foreignKeyName) {
						return null;
					}
				}, true)
				.<TableCreationColumnOption, String, String, TableCreationColumnOption>
						redirect(TableCreationColumnOption::references, (tableName, referencedColumName) -> {
							table.addForeignKey(tableName).addColumnReference(newColumn.getName(), referencedColumName);
				})
				.<TableCreationColumnOption, String, String, String, TableCreationColumnOption>
						redirect(TableCreationColumnOption::references, (tableName, referencedColumName, foreignKeyColumnName) -> {
					NewTable.NewForeignKey newForeignKey = table.addForeignKey(tableName);
					newForeignKey.setName(foreignKeyColumnName);
					newForeignKey.addColumnReference(newColumn.getName(), referencedColumName);
				})
				.fallbackOn(this)
				.build(TableCreationColumnOption.class);
	}
	
	@Override
	public TableCreation setSchema(String schemaName) {
		table.setSchemaName(schemaName);
		return this;
	}
	
	@Override
	public TableCreation setCatalog(String catalogName) {
		table.setCatalogName(catalogName);
		return this;
	}
	
	@Override
	public TableCreation setPrimaryKey(String columnName, String... extraColumnNames) {
		table.setPrimaryKey(columnName, extraColumnNames);
		return this;
	}
	
	@Override
	public UniqueConstraintInTableCreation addUniqueConstraint(String columnName, String... extraColumnNames) {
		NewTable.NewUniqueConstraint uniqueConstraintSupport = table.addUniqueConstraint(columnName, extraColumnNames);
		return new MethodReferenceDispatcher()
				.redirect(UniqueConstraintInTableCreation::setUniqueConstraintName, uniqueConstraintSupport::setName)
				.fallbackOn(this)
				.build(UniqueConstraintInTableCreation.class);
	}
	
	@Override
	public ForeignKeyInTableCreation addForeignKey(String targetTableName) {
		NewTable.NewForeignKey foreignKeyCreationSupport = table.addForeignKey(targetTableName);
		return new MethodReferenceDispatcher()
				.redirect(ForeignKeyInTableCreation::addColumnReference, (s1, s2) -> { foreignKeyCreationSupport.addColumnReference(s1, s2); })
				.redirect(ForeignKeyInTableCreation::setForeignKeyName, s -> { foreignKeyCreationSupport.setName(s); })
				.fallbackOn(this)
				.build(ForeignKeyInTableCreation.class);
	}
	
	@Override
	public NewTable build() {
		return table;
	}
}
