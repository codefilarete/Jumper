package org.gama.jumper.ddl.engine;

import org.gama.jumper.ddl.dsl.support.NewTable;

/**
 * @author Guillaume Mary
 */
public class Dialect implements NewTableGenerator {
	
	private final NewTableHandler newTableHandler;
	
	public Dialect() {
		this.newTableHandler = new NewTableHandler();
	}
	
	@Override
	public String generateScript(NewTable table) {
		return newTableHandler.generateScript(table);
	}
}
