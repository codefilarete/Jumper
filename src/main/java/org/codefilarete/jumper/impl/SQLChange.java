package org.codefilarete.jumper.impl;

import java.util.Arrays;
import java.util.List;

import org.codefilarete.jumper.AbstractChange;
import org.codefilarete.jumper.ChangeId;

/**
 * An update dedicated to SQL execution
 * 
 * @author Guillaume Mary
 */
public class SQLChange extends AbstractChange {
	
	private final List<String> sqlOrders;
	
	public SQLChange(ChangeId changeId, String... sqlOrders) {
		this(changeId, false, sqlOrders);
	}
	
	public SQLChange(String identifier, String... sqlOrders) {
		this(identifier, false, sqlOrders);
	}
	
	public SQLChange(ChangeId changeId, boolean shouldAlwaysRun, String... sqlOrders) {
		this(changeId, shouldAlwaysRun, Arrays.asList(sqlOrders));
	}
	
	public SQLChange(String identifier, boolean shouldAlwaysRun, String... sqlOrders) {
		this(new ChangeId(identifier), shouldAlwaysRun, sqlOrders);
	}
	
	public SQLChange(ChangeId changeId, List<String> sqlOrders) {
		super(changeId, false);
		this.sqlOrders = sqlOrders;
	}
	
	public SQLChange(String identifier, List<String> sqlOrders) {
		super(identifier, false);
		this.sqlOrders = sqlOrders;
	}
	
	public SQLChange(ChangeId changeId, boolean shouldAlwaysRun, List<String> sqlOrders) {
		super(changeId, shouldAlwaysRun);
		this.sqlOrders = sqlOrders;
	}
	
	public SQLChange(String identifier, boolean shouldAlwaysRun, List<String> sqlOrders) {
		super(identifier, shouldAlwaysRun);
		this.sqlOrders = sqlOrders;
	}
	
	public List<String> getSqlOrders() {
		return this.sqlOrders;
	}
}
