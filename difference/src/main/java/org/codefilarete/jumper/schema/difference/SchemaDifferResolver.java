package org.codefilarete.jumper.schema.difference;

import java.sql.Connection;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;


public interface SchemaDifferResolver {
	
	/**
	 * Expected to give the {@link SchemaDiffer} to be used with a database
	 *
	 * @param conn an open connection to the database
	 * @return the most compatible dialect with given database connection
	 */
	SchemaDiffer determineSchemaDifference(Connection conn);
	
	/**
	 * @author Guillaume Mary
	 */
	interface SchemaDifferResolverEntry {
		
		DatabaseSignet getCompatibility();
		
		SchemaDiffer getSchemaDiffer();
		
	}
}
