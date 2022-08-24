package org.codefilarete.jumper.impl;

import java.util.Arrays;
import java.util.List;

import org.codefilarete.jumper.Change;

/**
 * An update dedicated to user-defined SQL execution
 * 
 * @author Guillaume Mary
 */
public class SQLChange implements Change {
	
	private final List<String> sqlOrders;
	
	public SQLChange(String... sqlOrders) {
		this(Arrays.asList(sqlOrders));
	}
	
	public SQLChange(List<String> sqlOrders) {
		this.sqlOrders = sqlOrders;
	}
	
	public List<String> getSqlOrders() {
		return this.sqlOrders;
	}
}
