package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.impl.SupportedChange;

/**
 * Global contract to generate some SQL from an object.
 *
 * @param <C> object type which represent the object to generate SQL for
 * @author Guillaume Mary
 */
public interface SQLGenerator<C extends SupportedChange> {
	
	String generateScript(C sqlObject);
}
