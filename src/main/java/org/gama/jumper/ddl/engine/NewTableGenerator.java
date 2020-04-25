package org.gama.jumper.ddl.engine;

import org.gama.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewTableGenerator {
	
	String generateScript(NewTable table);
}
