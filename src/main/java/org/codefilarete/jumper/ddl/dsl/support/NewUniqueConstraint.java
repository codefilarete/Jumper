package org.codefilarete.jumper.ddl.dsl.support;

import java.util.Arrays;

import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class NewUniqueConstraint {
	
	private final String name;
	
	private final KeepOrderSet<String> columns = new KeepOrderSet<>();
	
	public NewUniqueConstraint(String name, String columnName, String... extraColumnNames) {
		this.name = name;
		this.columns.add(columnName);
		this.columns.addAll(Arrays.asList(extraColumnNames));
	}
	
	public String getName() {
		return name;
	}
	
	public KeepOrderSet<String> getColumns() {
		return columns;
	}
}
