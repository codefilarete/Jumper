package org.codefilarete.jumper.ddl.dsl.support;

import java.util.ArrayList;

import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ChangeSetRunner;
import org.codefilarete.jumper.ChangeStorage;
import org.codefilarete.jumper.SeparateConnectionProvider;
import org.codefilarete.jumper.UpdateProcessSemaphore;
import org.codefilarete.jumper.ddl.dsl.Builder;
import org.codefilarete.jumper.ddl.dsl.FluentChangeLog;
import org.codefilarete.jumper.impl.JdbcChangeStorage;
import org.codefilarete.jumper.impl.JdbcUpdateProcessSemaphore;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.ArrayIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

public class FluentChangeLogSupport implements FluentChangeLog {
	
	private final KeepOrderSet<ChangeSet> changeSets = new KeepOrderSet<>();
	
	private ChangeStorage changeHistoryStorage;
	
	private UpdateProcessSemaphore processSemaphore;
	
	public FluentChangeLogSupport addBuilders(Builder<? extends ChangeSet>... changes) {
		return addBuilders(Iterables.asIterable(new ArrayIterator<>(changes)));
	}
	
	public FluentChangeLogSupport addBuilders(Iterable<? extends Builder<? extends ChangeSet>> changeSets) {
		Iterable<ChangeSet> collect = Iterables.collect(changeSets, Builder::build, ArrayList::new);
		return addAll(collect);
	}
	
	public FluentChangeLogSupport addAll(ChangeSet... changeSets) {
		return addAll(Iterables.asIterable(new ArrayIterator<>(changeSets)));
	}
	
	public FluentChangeLogSupport addAll(Iterable<? extends ChangeSet> changeSets) {
		Iterables.copy(changeSets, this.changeSets);
		return this;
	}
	
	public FluentChangeLogSupport withChangeHistoryStorage(ChangeStorage changeStorage) {
		this.changeHistoryStorage = changeStorage;
		return this;
	}
	
	public FluentChangeLogSupport withProcessSemaphore(UpdateProcessSemaphore processSemaphore) {
		this.processSemaphore = processSemaphore;
		return this;
	}
	
	@Override
	public void applyTo(ConnectionProvider connectionProvider) {
		// Fixing default value for changes history storage
		if (changeHistoryStorage == null) {
			changeHistoryStorage = new JdbcChangeStorage(connectionProvider);
		}
		// Fixing default value for process semaphore
		if (processSemaphore == null) {
			if (!(connectionProvider instanceof SeparateConnectionProvider)) {
				throw new UnsupportedOperationException("Jdbc lock storage requires a " + Reflections.toString(SeparateConnectionProvider.class) + " to acquire lock correctly");
			}
			processSemaphore = new JdbcUpdateProcessSemaphore((SeparateConnectionProvider) connectionProvider);
		}
		
		// Building final result
		ChangeSetRunner result = new ChangeSetRunner(new ArrayList<>(changeSets), connectionProvider, changeHistoryStorage, processSemaphore);
		// we add storage listeners so they can create their tables at very beginning of the process
		if (processSemaphore instanceof JdbcUpdateProcessSemaphore) {
			result.addExecutionListener(((JdbcUpdateProcessSemaphore) processSemaphore).getLockTableEnsurer());
		}
		if (changeHistoryStorage instanceof JdbcChangeStorage) {
			result.addExecutionListener(((JdbcChangeStorage) changeHistoryStorage).getChangeHistoryTableEnsurer());
		}
		result.processUpdate();
	}
}
