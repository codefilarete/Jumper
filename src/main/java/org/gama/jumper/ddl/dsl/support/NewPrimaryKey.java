package org.gama.jumper.ddl.dsl.support;

import java.util.Set;

import org.gama.lang.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewPrimaryKey {
	
	private final Set<NewColumn> columns = new KeepOrderSet<>();
	
	public void addColumn(NewColumn column) {
		this.columns.add(column);
	}
	
	public Set<NewColumn> getColumns() {
		return columns;
	}
}
