package org.codefilarete.jumper.ddl.dsl;

import org.codefilarete.jumper.ddl.dsl.support.*;

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
	
	public static ForeignKeyCreation createForeignKey(String name, String tableName) {
		return new ForeignKeyCreationSupport(name, tableName);
	}

	public static UniqueContraintCreation createUniqueContraint(String name, String tableName, String columnName, String... extraColumnNames) {
		return new UniqueContraintCreationSupport(name, tableName, columnName, extraColumnNames);
	}
}
