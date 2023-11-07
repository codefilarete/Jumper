package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.AddColumn;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface AddColumnGenerator extends SQLGenerator<AddColumn> {
	
	String generateScript(AddColumn addColumn);
}
