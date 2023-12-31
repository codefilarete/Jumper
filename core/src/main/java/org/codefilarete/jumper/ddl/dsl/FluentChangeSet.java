package org.codefilarete.jumper.ddl.dsl;

import java.util.function.Predicate;

import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.Context;

public interface FluentChangeSet extends Builder<ChangeSet> {
	
	FluentChangeSet runIf(Predicate<Context> contextCondition);
}
