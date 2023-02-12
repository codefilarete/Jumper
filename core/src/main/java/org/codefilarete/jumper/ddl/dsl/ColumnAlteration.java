package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;

/**
 * @author Guillaume Mary
 */
public interface ColumnAlteration extends ChangeSet.ChangeBuilder {

	ColumnAlteration notNull();

	ColumnAlteration autoIncrement();

	ColumnAlteration defaultValue(String defaultValue);

	ColumnAlteration setSchema(String schemaName);
	
	ColumnAlteration setCatalog(String catalogName);
	
	ModifyColumn build();
	
}
