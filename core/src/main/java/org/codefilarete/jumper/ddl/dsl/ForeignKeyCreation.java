package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;

/**
 * @author Guillaume Mary
 */
public interface ForeignKeyCreation extends FluentSupportedChange<NewForeignKey, ForeignKeyCreation> {
	
	ForeignKeyCreation addColumnReference(String sourceColumnName, String targetColumnName);
}
