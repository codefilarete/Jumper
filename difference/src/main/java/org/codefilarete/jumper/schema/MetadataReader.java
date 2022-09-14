package org.codefilarete.jumper.schema;

import java.util.Set;
import java.util.SortedSet;

public interface MetadataReader {
	
	SortedSet<ColumnMetadata> giveColumns(String catalog, String schema, String tablePattern);
	
	/**
	 * Gives foreign keys exported by tables matching given pattern
	 *
	 * @param catalog table catalog
	 * @param schema table schema
	 * @param tablePattern table pattern for which foreign keys must be retrieved
	 * @return foreign keys exported by tables matching given pattern
	 */
	Set<ForeignKeyMetadata> giveExportedKeys(String catalog, String schema, String tablePattern);
	
	/**
	 * Gives foreign keys pointing to primary of tables matching given pattern
	 *
	 * @param catalog table catalog
	 * @param schema table schema
	 * @param tablePattern table pattern which primary keys are target of foreign keys
	 * @return foreign keys pointing to primary of tables matching given pattern
	 */
	Set<ForeignKeyMetadata> giveImportedKeys(String catalog, String schema, String tablePattern);
	
	PrimaryKeyMetadata givePrimaryKey(String catalog, String schema, String tableName);
	
	Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern);
	
	Set<ViewMetadata> giveViews(String catalog, String schema, String tableNamePattern);
	
	Set<IndexMetadata> giveIndexes(String catalog, String schema, String tableName);
	
	Set<ProcedureMetadata> giveProcedures(String catalog, String schema, String procedurePatternName);
	
}
