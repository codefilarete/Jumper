package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.TableCreation.TableCreationColumnOption;

/**
 * @author Guillaume Mary
 */
public interface ColumnOption {
	
	ColumnOption notNull();
	
	ColumnOption autoIncrement();
	
	ColumnOption defaultValue(String defaultValue);
	
	TableCreationColumnOption uniqueConstraint(String name);
	
}
