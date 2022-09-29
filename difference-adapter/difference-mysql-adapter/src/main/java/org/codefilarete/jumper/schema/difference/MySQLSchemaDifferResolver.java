package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class MySQLSchemaDifferResolver {
	
	public static class MySQL_5_6_Entry implements SchemaDifferResolver.SchemaDifferResolverEntry {
		
		private static final MySQLSchemaDiffer MARIADB_SCHEMA_DIFFER = new MySQLSchemaDiffer();
		
		private static final DatabaseSignet MYSQL_5_6_SIGNET = new DatabaseSignet("MySQL", 5, 6);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return MYSQL_5_6_SIGNET;
		}
		
		@Override
		public MySQLSchemaDiffer getSchemaDiffer() {
			return MARIADB_SCHEMA_DIFFER;
		}
	}
}