package org.gama.jumper.ddl.engine;

import org.gama.jumper.ddl.dsl.support.NewForeignKey;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewForeignKeyGenerator {
	
	String generateScript(NewForeignKey foreignKey);
}
