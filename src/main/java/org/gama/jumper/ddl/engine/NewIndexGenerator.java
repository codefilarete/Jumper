package org.gama.jumper.ddl.engine;

import org.gama.jumper.ddl.dsl.support.NewIndex;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewIndexGenerator {
	
	String generateScript(NewIndex index);
}
