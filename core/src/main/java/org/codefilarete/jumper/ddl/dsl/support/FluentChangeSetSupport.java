package org.codefilarete.jumper.ddl.dsl.support;

import java.util.function.Predicate;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.Context;
import org.codefilarete.jumper.ddl.dsl.Builder;
import org.codefilarete.jumper.ddl.dsl.FluentChangeSet;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;

public class FluentChangeSetSupport implements FluentChangeSet {
	
	private final String changeSetId;
	private Predicate<Context> contextCondition;
	private final KeepOrderSet<Builder<? extends Change>> changes = new KeepOrderSet<>();
	
	public FluentChangeSetSupport(String changeSetId) {
		this.changeSetId = changeSetId;
	}
	
	@Override
	public FluentChangeSet runIf(Predicate<Context> contextCondition) {
		this.contextCondition = contextCondition;
		return this;
	}
	
	public FluentChangeSet addChanges(Builder<? extends Change>... changes) {
		this.changes.addAll(Arrays.asList(changes));
		return this;
	}
	
	@Override
	public ChangeSet build() {
		ChangeSet result = new ChangeSet(changeSetId);
		if (this.contextCondition != null) {
			result.runIf(this.contextCondition);
		}
		result.addChanges(this.changes.toArray(new Builder[0]));
		return result;
	}
}