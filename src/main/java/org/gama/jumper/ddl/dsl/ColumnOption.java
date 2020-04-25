package org.gama.jumper.ddl.dsl;

/**
 * @author Guillaume Mary
 */
public interface ColumnOption {
	
	ColumnOption notNull();
	
	ColumnOption autoIncrement();
	
	ColumnOption defaultValue(String defaultValue);
}
