package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface NewForeignKeyGenerator extends SQLGenerator<NewForeignKey> {
	
	String generateScript(NewForeignKey foreignKey);
}
