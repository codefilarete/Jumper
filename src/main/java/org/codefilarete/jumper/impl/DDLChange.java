package org.codefilarete.jumper.impl;

import org.codefilarete.jumper.AbstractChange;
import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;

/**
 * A change for any {@link DDLStatement}
 * 
 * @author Guillaume Mary
 */
public class DDLChange extends AbstractChange {
	
	private final DDLStatement ddlStatement;
	
	/**
	 * Constructor with mandatory arguments
	 * 
	 * @param identifier identifier of this change
	 * @param ddlStatement DDL to be run
	 */
	public DDLChange(String identifier, DDLStatement ddlStatement) {
		super(identifier, false);	// structure changes are expected to be run once
		this.ddlStatement = ddlStatement;
	}
	
	public DDLStatement getDdlStatement() {
		return ddlStatement;
	}
}
