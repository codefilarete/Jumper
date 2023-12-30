package org.codefilarete.jumper;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * Class aimed at storing information about context of {@link Change}s execution.
 *
 * Default gives {@link DatabaseSignet} and ran {@link ChangeSetId}s.
 * Instantiation, therefore its content creation, can be overridden through {@link ChangeSetRunner#buildContext(Connection, Set)}.
 *
 * @author Guillaume Mary
 */
public class Context {
	
	private final DatabaseSignet databaseSignet;
	
	private final Set<ChangeSetId> alreadyRanChanges;
	
	public Context(DatabaseSignet databaseSignet, Set<ChangeSetId> alreadyRanChanges) {
		this.databaseSignet = databaseSignet;
		// input may give unmodifiable Set and since current class requires it to be modifiable we transfer it
		// to a modifiable Set
		this.alreadyRanChanges = new HashSet<>(alreadyRanChanges);
	}
	
	public DatabaseSignet getDatabaseSignet() {
		return databaseSignet;
	}
	
	/**
	 * Gives changes identifiers present in database before the execution of a new set of changes
	 * (before {@link ChangeSetRunner} execution)
	 * @return changes identifiers present in database, unmodifiable.
	 */
	public Set<ChangeSetId> getAlreadyRanChanges() {
		return alreadyRanChanges;
	}
	
	/**
	 * Add ran {@link ChangeSet} identifier to the list of executed ones in order to be up-to-date for next calls to
	 * {@link #getAlreadyRanChanges()} and mainly for {@link Predicate} on it like {@link org.codefilarete.jumper.ChangeSet.ExecutedChangeSetPredicate}
	 * It is called by {@link ChangeSetRunner} after each {@link ChangeSet} execution
	 *
	 * @param changeSet
	 * @see ChangeSetRunner.ChangeRunner#run(ChangeSet, Context)
	 */
	protected void addRanChangeSet(ChangeSet changeSet) {
		this.alreadyRanChanges.add(changeSet.getIdentifier());
	}
}
