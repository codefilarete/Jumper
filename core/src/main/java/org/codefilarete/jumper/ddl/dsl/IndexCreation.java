package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.NewIndex;

/**
 * @author Guillaume Mary
 */
public interface IndexCreation extends FluentSupportedChange<NewIndex, IndexCreation> {
	
	IndexCreation addColumn(String name);
	
	IndexCreation unique();
}
