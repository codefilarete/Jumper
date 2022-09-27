package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class MariaDBSchemaDifferResolver {
	
	public static class MariaDB_10_0_Entry implements SchemaDifferResolver.SchemaDifferResolverEntry {
		
		private static final MariaDBSchemaDiffer MARIADB_SCHEMA_DIFFER = new MariaDBSchemaDiffer();
		
		private static final DatabaseSignet MARIADB_10_0_SIGNET = new DatabaseSignet("MariaDB", 10, 0);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return MARIADB_10_0_SIGNET;
		}
		
		@Override
		public MariaDBSchemaDiffer getSchemaDiffer() {
			return MARIADB_SCHEMA_DIFFER;
		}
	}
}