package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class HSQLDBSchemaDifferResolver {
	
	public static class HSQLDB_2_0_Entry implements SchemaDifferResolver.SchemaDifferResolverEntry {
		
		private static final HSQLDBSchemaDiffer HSQLDB_DIALECT = new HSQLDBSchemaDiffer();
		
		private static final DatabaseSignet HSQL_2_0_SIGNET = new DatabaseSignet("HSQL Database Engine", 2, 0);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return HSQL_2_0_SIGNET;
		}
		
		@Override
		public SchemaDiffer getSchemaDiffer() {
			return HSQLDB_DIALECT;
		}
	}
}