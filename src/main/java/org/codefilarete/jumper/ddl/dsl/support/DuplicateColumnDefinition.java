package org.codefilarete.jumper.ddl.dsl.support;

/**
 * @author Guillaume Mary
 */
public class DuplicateColumnDefinition extends RuntimeException {
	
	private final NewColumn newColumn;
	
	public DuplicateColumnDefinition(NewColumn newColumn) {
		this.newColumn = newColumn;
	}
}
