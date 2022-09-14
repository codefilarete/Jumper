package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewForeignKeyGenerator {
	
	String generateScript(NewForeignKey foreignKey);
}
