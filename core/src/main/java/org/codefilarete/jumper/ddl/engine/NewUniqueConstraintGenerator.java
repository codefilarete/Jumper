package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewUniqueConstraint;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewUniqueConstraintGenerator extends SQLGenerator<NewUniqueConstraint> {
	
	String generateScript(NewUniqueConstraint newUniqueConstraint);
}
