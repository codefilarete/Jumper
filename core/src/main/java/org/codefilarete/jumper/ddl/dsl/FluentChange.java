package org.codefilarete.jumper.ddl.dsl;

import java.util.function.Predicate;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.Context;

public interface FluentChange<C extends Change, SELF extends FluentChange<C, SELF>> extends Builder<C> {
	
	/**
	 * Gives a condition on which this change will be applied if it is verified.
	 * Note that for a wider-grained condition, one can use {@link org.codefilarete.jumper.ChangeSet#runIf(Predicate)}.
	 *
	 * @param contextCondition a {@link Predicate} which, if returns true, allows to run this instance
	 * @return this
	 */
	SELF runIf(Predicate<Context> contextCondition);
}