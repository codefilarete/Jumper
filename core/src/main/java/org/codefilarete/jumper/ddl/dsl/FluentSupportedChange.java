package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.impl.SupportedChange;

public interface FluentSupportedChange<C extends SupportedChange, SELF extends FluentSupportedChange<C, SELF>> extends FluentChange<C, SELF> {
	
	SELF setSchema(String schemaName);
	
	SELF setCatalog(String catalogName);
}
