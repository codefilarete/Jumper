package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewTableGenerator extends SQLGenerator<NewTable> {
	
	String generateScript(NewTable table);
}
