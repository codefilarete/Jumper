package org.codefilarete.jumper.ddl.dsl;

/**
 * @author Guillaume Mary
 */
public interface ColumnOption {
	
	/**
	 * Marks this column as being not null.
	 * According to database vendor, this may generate a not-null constraint.
	 *
	 * @return current column options object
	 */
	ColumnOption notNull();
	
	/**
	 * Marks this column as being auto-incremented.
	 *
	 * @return current column options object
	 */
	ColumnOption autoIncrement();
	
	/**
	 * Gives the default value of this column. It is expressed as a {@link String} which means that user should give
	 * the exact value to be put in generated SQL. For example "true" as a {@link String} for a boolean column.
	 *
	 * @param defaultValue the default value of the column
	 * @return current column options object
	 */
	ColumnOption defaultValue(String defaultValue);
	
	/**
	 * Marks this column as being part of the table primary key.
	 * Primary key columns are took in the order columns are declared onto the table, which can be no so robust, that's
	 * why this method was more though to be used for single-column primary key case to ease its declaration.
	 * But still, this method is usable for multi-column primary key.
	 *
	 * @return current column options object
	 */
	ColumnOption primaryKey();
	
	/**
	 * Marks this column as unique.
	 *
	 * @return current column options object
	 */
	ColumnOption unique();
	
	/**
	 * Marks this column as unique.
	 *
	 * @param uniqueConstraintName name of the constraint to be created
	 * @return current column options object
	 */
	ColumnOption unique(String uniqueConstraintName);
	
	/**
	 * Adds a foreign key between current column and the one described by arguments.
	 *
	 * @param tableName table of referenced column
	 * @param columnName referenced column in the referenced table
	 * @return current column options object
	 */
	ColumnOption references(String tableName, String columnName);
	
	/**
	 * Adds a named foreign key between current column and the one described by arguments.
	 * This method can be called multiple times to create a multiple-column foreign key if foreign key names are equals.
	 *
	 * @param tableName table of referenced column
	 * @param columnName referenced column in the referenced table
	 * @param foreignKeyName foreign key name
	 * @return current column options object
	 */
	ColumnOption references(String tableName, String columnName, String foreignKeyName);
	
}
