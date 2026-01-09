package org.codefilarete.jumper.schema.metadata;

import javax.annotation.Nullable;
import java.sql.DatabaseMetaData;
import java.util.Set;
import java.util.SortedSet;

public interface SchemaMetadataReader {
	
	DatabaseMetaData getMetaData();
	
	/**
	 * Retrieves the columns for tables matching the given parameters.
	 *
	 * @param catalog the catalog name; its case must match as it is stored in the database;
	 *                null means the catalog name should not be used to narrow the search.
	 * @param schema the schema name; its case must match as it is stored in the database;
	 *               null means the schema name should not be used to narrow the search.
	 * @param tableNamePattern a table name `like` pattern; must match the table name as it is stored in the database;
	 *                         null means the table name pattern should not be used to narrow the search.
	 * @return a sorted set of {@code ColumnMetadata} representing the columns of the matching tables.
	 */
	SortedSet<ColumnMetadata> giveColumns(@Nullable String catalog, @Nullable String schema, @Nullable String tableNamePattern);
	
	/**
	 * Gives foreign keys exported by tables matching given pattern
	 *
	 * @param catalog table catalog
	 * @param schema table schema
	 * @param tableNamePattern table pattern for which foreign keys must be retrieved
	 * @return foreign keys exported by tables matching given pattern
	 */
	Set<ForeignKeyMetadata> giveExportedKeys(String catalog, String schema, String tableNamePattern);
	
	/**
	 * Gives foreign keys pointing to primary of tables matching given pattern
	 *
	 * @param catalog table catalog
	 * @param schema table schema
	 * @param tableNamePattern table pattern which primary keys are target of foreign keys
	 * @return foreign keys pointing to primary of tables matching given pattern
	 */
	Set<ForeignKeyMetadata> giveImportedKeys(String catalog, String schema, String tableNamePattern);
	
	Set<PrimaryKeyMetadata> givePrimaryKeys(String catalog, String schema, String tableNamePattern);
	
	Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern);
	
	Set<ViewMetadata> giveViews(String catalog, String schema, String tableNamePattern);
	
	Set<UniqueConstraintMetadata> giveUniqueConstraints(String catalog, String schema, String tablePattern);
	
	/**
	 * Gives the indexes for tables matching the given pattern.
	 * If {@code unique} is not set, then both unique and non-unique indexes are returned.
	 *
	 * @param catalog the catalog name; must match the catalog name as it is stored in the database;
	 *                null means that the catalog name should not be used to narrow the search
	 * @param schema the schema name; must match the schema name as it is stored in the database;
	 *               null means that the schema name should not be used to narrow the search
	 * @param tableNamePattern a table name `like` pattern; must match the table name as it is stored in the database
	 * @param unique when true, return only indices for unique values;
	 *               when false, return only indices for non-unique values;
	 *               when null return indices regardless of whether unique or not
	 * @return a set of {@link IndexMetadata} for the matching tables
	 */
	Set<IndexMetadata> giveIndexes(@Nullable String catalog, @Nullable String schema, @Nullable String tableNamePattern, @Nullable Boolean unique);
	
	Set<ProcedureMetadata> giveProcedures(String catalog, String schema, String procedurePatternName);
	
}
