package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLSchemaDifferResolver {
	
	public static class PostgreSQL_13_6_Entry implements SchemaDifferResolver.SchemaDifferResolverEntry {
		
		private static final PostgreSQLSchemaDiffer POSTGRESQL_SCHEMA_DIFFER = new PostgreSQLSchemaDiffer();
		
		private static final DatabaseSignet POSTGRESQL_13_6_SIGNET = new DatabaseSignet("PostgreSQL", 13, 6);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return POSTGRESQL_13_6_SIGNET;
		}
		
		@Override
		public PostgreSQLSchemaDiffer getSchemaDiffer() {
			return POSTGRESQL_SCHEMA_DIFFER;
		}
	}
}