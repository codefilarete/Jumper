package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewIndex;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewIndexGenerator extends SQLGenerator<NewIndex> {
	
	String generateScript(NewIndex index);
}
