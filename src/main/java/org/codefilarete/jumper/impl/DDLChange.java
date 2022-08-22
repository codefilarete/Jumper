package org.codefilarete.jumper.impl;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.jumper.AbstractChange;
import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * A change for any {@link DDLStatement}
 * 
 * @author Guillaume Mary
 */
public class DDLChange extends AbstractChange {
	
	private final List<DDLStatement> ddlStatements;
	
	/**
	 * Constructor with mandatory arguments
	 * 
	 * @param identifier identifier of this change
	 * @param ddlStatements DDL to be run
	 */
	public DDLChange(String identifier, DDLStatement... ddlStatements) {
		this(identifier, Arrays.asList(ddlStatements));
	}
	
	public DDLChange(String identifier, Iterable<DDLStatement> ddlStatements) {
		super(identifier, false);	// structure changes are expected to be run once
		this.ddlStatements = Iterables.copy(ddlStatements, new ArrayList<>());
	}
	
	public List<DDLStatement> getDdlStatements() {
		return ddlStatements;
	}
}
