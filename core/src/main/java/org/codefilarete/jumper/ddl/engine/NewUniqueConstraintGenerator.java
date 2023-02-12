package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewUniqueConstraint;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewUniqueConstraintGenerator {
	
	String generateScript(NewUniqueConstraint newUniqueConstraint);
}
