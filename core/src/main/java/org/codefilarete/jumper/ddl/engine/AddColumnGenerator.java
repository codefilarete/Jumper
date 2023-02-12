package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.AddColumn;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface AddColumnGenerator {
	
	String generateScript(AddColumn addColumn);
}
