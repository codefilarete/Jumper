package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ChangeSet;

/**
 * @author Guillaume Mary
 */
public interface UniqueContraintCreation extends ChangeSet.ChangeBuilder {

	UniqueContraintCreation setSchema(String schemaName);

	UniqueContraintCreation setCatalog(String catalogName);
}
