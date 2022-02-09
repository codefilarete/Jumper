package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.NewIndex;

/**
 * @author Guillaume Mary
 */
public interface IndexCreation {
	
	IndexCreation addColumn(String name);
	
	IndexCreation unique();
	
	IndexCreation setSchema(String schema);
	
	IndexCreation setCatalog(String schema);
	
	NewIndex build();
}
