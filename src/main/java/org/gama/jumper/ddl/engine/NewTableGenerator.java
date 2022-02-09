package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewTableGenerator {
	
	String generateScript(NewTable table);
}
