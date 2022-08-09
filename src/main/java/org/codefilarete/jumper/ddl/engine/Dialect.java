package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
public class Dialect implements NewTableGenerator {
	
	private final NewTableHandler newTableHandler;
	private final NewForeignKeyHandler newForeignKeyHandler;
	private final NewIndexHandler newIndexHandler;
	
	public Dialect() {
		this.newTableHandler = new NewTableHandler();
		this.newForeignKeyHandler = new NewForeignKeyHandler();
		this.newIndexHandler = new NewIndexHandler();
	}
	
	@Override
	public String generateScript(NewTable table) {
		return newTableHandler.generateScript(table);
	}
}
