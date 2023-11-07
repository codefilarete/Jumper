package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;

/**
 * @author Guillaume Mary
 */
public interface ColumnAlteration extends FluentSupportedChange<ModifyColumn, ColumnAlteration> {
	
	ColumnAlteration notNull();
	
	ColumnAlteration autoIncrement();
	
	ColumnAlteration defaultValue(String defaultValue);
	
}
