package org.codefilarete.jumper.impl;

import org.codefilarete.jumper.AbstractChange;
import org.codefilarete.jumper.ChangeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An update dedicated to SQL execution
 * 
 * @author Guillaume Mary
 */
public class SQLChange extends AbstractChange {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SQLChange.class);
	
	private final String[] sqlOrders;
	
	public SQLChange(ChangeId changeId, boolean shouldAlwaysRun, String[] sqlOrders) {
		super(changeId, shouldAlwaysRun);
		this.sqlOrders = sqlOrders;
	}
	
	public SQLChange(String identifier, boolean shouldAlwaysRun, String[] sqlOrders) {
		this(new ChangeId(identifier), shouldAlwaysRun, sqlOrders);
	}
	
	public String[] getSqlOrders() {
		return this.sqlOrders;
	}
}
