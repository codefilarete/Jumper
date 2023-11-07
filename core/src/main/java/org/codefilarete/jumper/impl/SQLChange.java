package org.codefilarete.jumper.impl;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.Context;

/**
 * An update dedicated to user-defined SQL execution
 * 
 * @author Guillaume Mary
 */
public class SQLChange implements Change {
	
	private final List<String> sqlOrders;
	
	/** By default the change is run */
	private Predicate<Context> runCondition = context -> true;
	
	public SQLChange(String... sqlOrders) {
		this(Arrays.asList(sqlOrders));
	}
	
	public SQLChange(List<String> sqlOrders) {
		this.sqlOrders = sqlOrders;
	}
	
	public List<String> getSqlOrders() {
		return this.sqlOrders;
	}
	
	public SQLChange runIf(Predicate<Context> contextCondition) {
		this.runCondition = contextCondition;
		return this;
	}
	
	/**
	 * Indicates if this {@link ChangeSet} must be run on the given {@link Context}. Default is yes (true).
	 * Use {@link #runIf(Predicate)} to give a conditional reason of execution according to {@link Context}.
	 */
	public boolean shouldRun(Context context) {
		return runCondition.test(context);
	}
}
