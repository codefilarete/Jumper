package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ddl.dsl.support.AddColumn;

/**
 * @author Guillaume Mary
 */
public interface ColumnAddition extends ChangeSet.ChangeBuilder {
	
	ColumnAddition notNull();
	
	ColumnAddition autoIncrement();
	
	ColumnAddition defaultValue(String defaultValue);
	
	ColumnAddition setSchema(String schemaName);
	
	ColumnAddition setCatalog(String catalogName);
	
	AddColumn build();
	
}
