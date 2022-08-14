package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.DropTable;

/**
 *
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface DropTableGenerator {
	
	String generateScript(DropTable dropTable);
}
