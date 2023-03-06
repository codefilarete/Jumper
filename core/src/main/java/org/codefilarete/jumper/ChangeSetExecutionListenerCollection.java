package org.codefilarete.jumper;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.jumper.ChangeSetExecutionListener.StatementExecutionListener;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 *
 * @author Guillaume Mary
 */
public class ChangeSetExecutionListenerCollection implements ChangeSetExecutionListener, StatementExecutionListener {
	
	private final List<ChangeSetExecutionListener> listeners;
	
	public ChangeSetExecutionListenerCollection(Iterable<ChangeSetExecutionListener> listeners) {
		this.listeners = Iterables.copy(listeners, new ArrayList<>());
	}
	
	public ChangeSetExecutionListenerCollection(ChangeSetExecutionListener... listeners) {
		this.listeners = Arrays.asList(listeners);
	}
	
	public void add(ChangeSetExecutionListener executionListener) {
		this.listeners.add(executionListener);
	}
	
	@Override
	public void beforeRun(ChangeSet changes) {
		this.listeners.forEach(l -> l.beforeRun(changes));
	}
	
	@Override
	public void afterRun(ChangeSet changes) {
		this.listeners.forEach(l -> l.afterRun(changes));
	}
	
	@Override
	public void beforeRun(Change change) {
		this.listeners.forEach(l -> l.beforeRun(change));
	}
	
	@Override
	public void afterRun(Change change) {
		this.listeners.forEach(l -> l.afterRun(change));
	}
	
	@Override
	public void afterProcess() {
		this.listeners.forEach(ChangeSetExecutionListener::afterProcess);
	}
	
	@Override
	public void beforeProcess() {
		this.listeners.forEach(ChangeSetExecutionListener::beforeProcess);
	}
	
	@Override
	public void afterRun(String statement, Long updatedRowCount) {
		this.listeners.stream().filter(StatementExecutionListener.class::isInstance)
				.map(StatementExecutionListener.class::cast)
				.forEach(l -> l.afterRun(statement, updatedRowCount));
	}
}
