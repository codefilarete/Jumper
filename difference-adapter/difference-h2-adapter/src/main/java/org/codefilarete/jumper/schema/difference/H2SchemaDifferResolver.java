package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class H2SchemaDifferResolver {
	
	public static class H2_1_4_Entry implements SchemaDifferResolver.SchemaDifferResolverEntry {
		
		private static final H2SchemaDiffer H2_SCHEMA_DIFFER = new H2SchemaDiffer();
		
		private static final DatabaseSignet H2_1_4_SIGNET = new DatabaseSignet("H2", 1, 4);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return H2_1_4_SIGNET;
		}
		
		@Override
		public SchemaDiffer getSchemaDiffer() {
			return H2_SCHEMA_DIFFER;
		}
	}
}