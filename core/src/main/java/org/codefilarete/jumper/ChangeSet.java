package org.codefilarete.jumper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;
import org.codefilarete.jumper.ddl.dsl.FluentChange;
import org.codefilarete.tool.function.Predicates;

/**
 * @author Guillaume Mary
 */
public class ChangeSet {
	
	public static ChangeSet changeSet(String changeSetId, FluentChange... changes) {
		return new ChangeSet(changeSetId).addChanges(changes);
	}
	
	private final ChangeSetId changeSetId;
	private boolean shouldAlwaysRun = false;
	private final List<Change> changes = new ArrayList<>();
	
	/** By default there's no condition on ChangeSet run, hence this returns true */
	private Predicate<Context> runCondition = context -> true;
	
	public ChangeSet(ChangeSetId changeSetId) {
		this.changeSetId = changeSetId;
	}
	
	public ChangeSet(String identifier) {
		this(new ChangeSetId(identifier));
	}
	
	public ChangeSetId getIdentifier() {
		return changeSetId;
	}
	
	public List<Change> getChanges() {
		return changes;
	}
	
	public ChangeSet addChanges(Change... changes) {
		this.changes.addAll(Arrays.asList(changes));
		return this;
	}
	
	public ChangeSet addChanges(ChangeBuilder... changes) {
		this.changes.addAll(Arrays.stream(changes).map(ChangeBuilder::build).collect(Collectors.toList()));
		return this;
	}
	
	/**
	 * Indicates if this {@link ChangeSet} must be executed even if it was already ran. Default is no (false).
	 */
	public boolean shouldAlwaysRun() {
		return shouldAlwaysRun;
	}
	
	/**
	 * Marks this {@link ChangeSet} to be executed even if it was already ran or not.
	 *
	 * @param yeOrNo true for changeSet to be executed even if it was already ran, false to run only once.
	 */
	public ChangeSet alwaysRun(boolean yeOrNo) {
		shouldAlwaysRun = yeOrNo;
		return this;
	}
	
	/**
	 * Gives a condition on which all changes of this instance will be applied if it is verified.
	 *
	 * @param contextCondition a {@link Predicate} which, if returns true, allows to run this instance
	 * @return this
	 * @see VendorPredicate
	 */
	public ChangeSet runIf(Predicate<Context> contextCondition) {
		this.runCondition = contextCondition;
		return this;
	}
	
	/**
	 * Indicates if this {@link ChangeSet} must be run on the given {@link Context}. Default is yes (true).
	 * Use {@link #runIf(Predicate)} to give a conditional reason of execution according to {@link Context}.
	 */
	boolean shouldRun(Context context) {
		return runCondition.test(context);
	}
	
	/**
	 * Interface for {@link ChangeSet}s that are signed with a MD5, SHA1, or whatever.
	 * Aimed at being used to check if this {@link ChangeSet} has changed since previous execution. So storage must record signature.
	 */
	Set<Checksum> getCompatibleChecksums() {
		return Collections.emptySet();
	}
	
	public interface ChangeBuilder<C extends Change> {
		
		C build();
	}
	
	/**
	 * Class that helps to create a condition on database vendor name to be used by {@link ChangeSet#runIf(Predicate)}.
	 *
	 * @author Guillaume Mary
	 * @see #DBMS_IS_ORACLE
	 * @see #DBMS_IS_MYSQL
	 * @see #DBMS_IS_MARIADB
	 */
	public static class VendorPredicate implements Predicate<DatabaseSignet> {
		
		public static final Predicate<Context> DBMS_IS_ORACLE = Predicates.predicate(Context::getDatabaseSignet, new VendorPredicate("Oracle"));
		public static final Predicate<Context> DBMS_IS_MYSQL = Predicates.predicate(Context::getDatabaseSignet, new VendorPredicate("MySQL"));
		public static final Predicate<Context> DBMS_IS_MARIADB = Predicates.predicate(Context::getDatabaseSignet, new VendorPredicate("MariaDB"));
		
		private final Set<String> expectedVendor;
		
		public VendorPredicate(String... expectedVendor) {
			this(new HashSet<>(Arrays.asList(expectedVendor)));
		}
		
		public VendorPredicate(Set<String> expectedVendor) {
			this.expectedVendor = expectedVendor.stream().map(String::toLowerCase).collect(Collectors.toSet());
		}
		
		@Override
		public boolean test(DatabaseSignet databaseSignet) {
			return this.expectedVendor.stream().anyMatch(databaseSignet.getProductName().toLowerCase()::contains);
		}
	}
}
