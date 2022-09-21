package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class DerbySchemaDifferResolver {
	
	public static class Derby_10_14_Entry implements SchemaDifferResolver.SchemaDifferResolverEntry {
		
		private static final DerbySchemaDiffer DERBY_DIALECT = new DerbySchemaDiffer();
		
		private static final DatabaseSignet DERBY_10_14_SIGNET = new DatabaseSignet("Apache Derby", 10, 14);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return DERBY_10_14_SIGNET;
		}
		
		@Override
		public SchemaDiffer getSchemaDiffer() {
			return DERBY_DIALECT;
		}
	}
}