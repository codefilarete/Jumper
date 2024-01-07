package org.codefilarete.jumper.benchmark;

import java.sql.DatabaseMetaData;

/**
 * A class mixing {@link DatabaseMetaData} and {@link DatabaseSchemaMetaData} to ease methods of {@link DatabaseMetaData}
 * related to schema discovery to be spied.
 *
 * @author Guillaume Mary
 */
public interface DatabaseSchemaMetaDataMashup extends DatabaseSchemaMetaData, DatabaseMetaData {

}
