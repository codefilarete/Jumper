package org.codefilarete.jumper.ddl.dsl;

import java.util.function.Predicate;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.ChangeSet.ChangeBuilder;
import org.codefilarete.jumper.Context;

public interface FluentChange<C extends Change, SELF extends FluentChange<C, SELF>> extends ChangeBuilder<C> {
	
	SELF runIf(Predicate<Context> contextPredicate);
}
