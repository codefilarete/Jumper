package org.gama.jumper.ddl.dsl;

import org.gama.jumper.ddl.dsl.support.ForeignKeyCreationSupport;
import org.gama.jumper.ddl.dsl.support.IndexCreationSupport;
import org.gama.jumper.ddl.dsl.support.TableCreationSupport;

/**
 * @author Guillaume Mary
 */
public class DDLEase {
	
	public static TableCreation createTable(String name) {
		return new TableCreationSupport(name);
	}
	
	public static IndexCreation createIndex(String name, String tableName) {
		return new IndexCreationSupport(name, tableName);
	}
	
	public static ForeignKeyCreation createForeignKey(String name, String tableName) {
		return new ForeignKeyCreationSupport(name, tableName);
	}
}
