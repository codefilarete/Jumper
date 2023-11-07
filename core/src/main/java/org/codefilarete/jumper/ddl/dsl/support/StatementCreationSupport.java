package org.codefilarete.jumper.ddl.dsl.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.codefilarete.jumper.Context;
import org.codefilarete.jumper.ddl.dsl.StatementCreation;
import org.codefilarete.jumper.impl.SQLChange;

/**
 * Storage for user-defined SQL statement.
 *
 * @author Guillaume Mary
 */
public class StatementCreationSupport implements StatementCreation {
	
	private Predicate<Context> contextPredicate;
	
	private final List<String> statements = new ArrayList<>();
	
	public StatementCreationSupport(String... statements) {
		this.statements.addAll(Arrays.asList(statements));
	}
	
	@Override
	public StatementCreation runIf(Predicate<Context> contextPredicate) {
		this.contextPredicate = contextPredicate;
		return this;
	}
	
	@Override
	public SQLChange build() {
		return new SQLChange(statements).runIf(contextPredicate);
	}
}
