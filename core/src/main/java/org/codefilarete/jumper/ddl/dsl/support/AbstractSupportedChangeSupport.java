package org.codefilarete.jumper.ddl.dsl.support;

import java.util.function.Predicate;

import org.codefilarete.jumper.Context;
import org.codefilarete.jumper.ddl.dsl.FluentSupportedChange;
import org.codefilarete.jumper.impl.SupportedChange;

/**
 * Base class to store configuration made through fluent API
 * @param <C>
 * @param <SELF>
 * @author Guillaume Mary
 */
public abstract class AbstractSupportedChangeSupport<C extends SupportedChange, SELF extends FluentSupportedChange<C, SELF>>
		implements FluentSupportedChange<C, SELF> {
	
	protected Predicate<Context> contextPredicate;
	
	@Override
	public SELF runIf(Predicate<Context> contextPredicate) {
		this.contextPredicate = contextPredicate;
		return (SELF) this;
	}
}
