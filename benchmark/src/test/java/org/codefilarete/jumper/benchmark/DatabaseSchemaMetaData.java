package org.codefilarete.jumper.benchmark;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface duplicating {@link java.sql.DatabaseMetaData} methods focusing on schema discovery.
 * Made to listen to those methods and decorate them with a chronometer to get the time they take to run.
 *
 * @author Guillaume Mary
 */
public interface DatabaseSchemaMetaData {
	
	ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException;
	
	ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException;
	
	ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException;
	
	ResultSet getSchemas() throws SQLException;
	
	ResultSet getCatalogs() throws SQLException;
	
	ResultSet getTableTypes() throws SQLException;
	
	ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException;
	
	ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException;
	
	ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException;
	
	ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException;
	
	ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException;
	
	ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException;
	
	ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException;
	
	ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException;
	
	ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException;
	
	ResultSet getTypeInfo() throws SQLException;
	
	ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException;
}
