package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewIndex;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewIndexGenerator {
	
	String generateScript(NewIndex index);
}
