package org.codefilarete.jumper.ddl.dsl;

import java.util.function.Predicate;
import java.util.stream.Stream;

import org.codefilarete.jumper.ChangeSet.ExecutedChangeSetPredicate;
import org.codefilarete.jumper.ChangeSet.VendorPredicate;
import org.codefilarete.jumper.ChangeSetId;
import org.codefilarete.jumper.Context;
import org.codefilarete.jumper.ddl.dsl.support.*;
import org.codefilarete.tool.function.Predicates;

/**
 * @author Guillaume Mary
 */
public class DDLEase {
	
	public static TableCreation createTable(String name) {
		return new TableCreationSupport(name);
	}
	
	public static ColumnAlteration modifyColumn(String tableName, String columnName, String sqlType) {
		return new ColumnAlterationSupport(tableName, columnName, sqlType);
	}
	
	public static ColumnAlteration modifyColumn(String tableName, String columnName, String sqlType, String extraArguments) {
		return new ColumnAlterationSupport(tableName, columnName, sqlType, extraArguments);
	}
	
	public static ColumnAdditionSupport addColumn(String tableName, String columnName, String sqlType) {
		return new ColumnAdditionSupport(tableName, columnName, sqlType);
	}
	
	public static ColumnAdditionSupport addColumn(String tableName, String columnName, String sqlType, String extraArguments) {
		return new ColumnAdditionSupport(tableName, columnName, sqlType, extraArguments);
	}
	
	public static IndexCreation createIndex(String name, String tableName) {
		return new IndexCreationSupport(name, tableName);
	}
	
	public static ForeignKeyCreation createForeignKey(String name, String sourceTableName, String targetTableName) {
		return new ForeignKeyCreationSupport(name, sourceTableName, targetTableName);
	}
	
	public static UniqueContraintCreation createUniqueConstraint(String name, String tableName, String columnName, String... extraColumnNames) {
		return new UniqueContraintCreationSupport(name, tableName, columnName, extraColumnNames);
	}
	
	public static StatementCreation sql(String... statements) {
		return new StatementCreationSupport(statements);
	}
	
	/**
	 * Condition that make {@link org.codefilarete.jumper.ChangeSet} or {@link FluentChange} to be run only on Oracle databases.
	 * @see org.codefilarete.jumper.ChangeSet#runIf(Predicate)
	 * @see FluentChange#runIf(Predicate)
	 */
	public static final Predicate<Context> DBMS_IS_ORACLE = Predicates.predicate(Context::getDatabaseSignet, new VendorPredicate("Oracle"));
	
	/**
	 * Condition that make {@link org.codefilarete.jumper.ChangeSet} or {@link FluentChange} to be run only on MySQL databases.
	 * @see org.codefilarete.jumper.ChangeSet#runIf(Predicate)
	 * @see FluentChange#runIf(Predicate)
	 */
	public static final Predicate<Context> DBMS_IS_MYSQL = Predicates.predicate(Context::getDatabaseSignet, new VendorPredicate("MySQL"));
	
	/**
	 * Condition that make {@link org.codefilarete.jumper.ChangeSet} or {@link FluentChange} to be run only on MariaDB databases.
	 * @see org.codefilarete.jumper.ChangeSet#runIf(Predicate)
	 * @see FluentChange#runIf(Predicate)
	 */
	public static final Predicate<Context> DBMS_IS_MARIADB = Predicates.predicate(Context::getDatabaseSignet, new VendorPredicate("MariaDB"));
	
	/**
	 * Condition that make {@link org.codefilarete.jumper.ChangeSet} or {@link FluentChange} to be run only if a
	 * {@link org.codefilarete.jumper.ChangeSet} has already been executed.
	 * To be combined with {@link Predicates#not(Predicate)} or {@link Predicate#negate()} (depending on your style of
	 * writing) if one wants its update to be applied if given change hasn't been executed.
	 *
	 * @see org.codefilarete.jumper.ChangeSet#runIf(Predicate)
	 * @see FluentChange#runIf(Predicate)
	 */
	public static Predicate<Context> executedChangesContains(String... expectedChangeSetIds) {
		ChangeSetId[] changeSetIds = Stream.of(expectedChangeSetIds).map(ChangeSetId::new).toArray(ChangeSetId[]::new);
		return Predicates.predicate(Context::getAlreadyRanChanges, new ExecutedChangeSetPredicate(changeSetIds));
	}
}
