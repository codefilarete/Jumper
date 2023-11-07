package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.AddColumn;

/**
 * @author Guillaume Mary
 */
public interface ColumnAddition extends FluentSupportedChange<AddColumn, ColumnAddition> {
	
	ColumnAddition notNull();
	
	ColumnAddition autoIncrement();
	
	ColumnAddition defaultValue(String defaultValue);
	
}
