package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface ModifyColumnGenerator {
	
	String generateScript(ModifyColumn modifyColumn);
}
