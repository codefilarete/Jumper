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
	
	Set<TypeInfo> giveColumnTypes();
	
	Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern);
	
	Set<ViewMetadata> giveViews(String catalog, String schema, String tableNamePattern);
	
	Set<IndexMetadata> giveIndexes(String catalog, String schema, String tableName);
	
	Set<ProcedureMetadata> giveProcedures(String catalog, String schema, String procedurePatternName);
	
	class TypeInfo {
		//		TYPE_NAME String => Type name
		private final String name;
		//		DATA_TYPE int => SQL data type from java.sql.Types
		private final int type;
		//		PRECISION int => maximum precision
		private int precision;
		//		LITERAL_PREFIX String => prefix used to quote a literal (may be null)
		private String literalPrefix;
		//		LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
		private String literalSuffix;
		//		CREATE_PARAMS String => parameters used in creating the type (may be null)
		private String createParams;
		//		NULLABLE short => can you use NULL for this type
		//				typeNoNulls - does not allow NULL values
		//				typeNullable - allows NULL values
		//				typeNullableUnknown - nullability unknown
		private boolean nullable;
		//		CASE_SENSITIVE boolean=> is it case sensitive.
		private boolean caseSensitive;
//		SEARCHABLE short => can you use "WHERE" based on this type:
//				typePredNone - No support
//				typePredChar - Only supported with WHERE .. LIKE
//				typePredBasic - Supported except for WHERE .. LIKE
//				typeSearchable - Supported for all WHERE ..
//		UNSIGNED_ATTRIBUTE boolean => is it unsigned.
//		FIXED_PREC_SCALE boolean => can it be a money value.
//		AUTO_INCREMENT boolean => can it be used for an auto-increment value.
		
		private boolean autoIncrementable;
//		LOCAL_TYPE_NAME String => localized version of type name (may be null)
//		MINIMUM_SCALE short => minimum scale supported
//		MAXIMUM_SCALE short => maximum scale supported
//		SQL_DATA_TYPE int => unused
//		SQL_DATETIME_SUB int => unused
//		NUM_PREC_RADIX int => usually 2 or 10
		
		TypeInfo(String name, int type) {
			this.name = name;
			this.type = type;
		}
		
		public String getName() {
			return name;
		}
		
		public int getType() {
			return type;
		}
		
		public int getPrecision() {
			return precision;
		}
		
		public void setPrecision(int precision) {
			this.precision = precision;
		}
		
		public String getLiteralPrefix() {
			return literalPrefix;
		}
		
		public void setLiteralPrefix(String literalPrefix) {
			this.literalPrefix = literalPrefix;
		}
		
		public String getLiteralSuffix() {
			return literalSuffix;
		}
		
		public void setLiteralSuffix(String literalSuffix) {
			this.literalSuffix = literalSuffix;
		}
		
		public String getCreateParams() {
			return createParams;
		}
		
		public void setCreateParams(String createParams) {
			this.createParams = createParams;
		}
		
		public boolean isNullable() {
			return nullable;
		}
		
		public void setNullable(boolean nullable) {
			this.nullable = nullable;
		}
		
		public boolean isCaseSensitive() {
			return caseSensitive;
		}
		
		public void setCaseSensitive(boolean caseSensitive) {
			this.caseSensitive = caseSensitive;
		}
		
		public boolean isAutoIncrementable() {
			return autoIncrementable;
		}
		
		public void setAutoIncrementable(boolean autoIncrementable) {
			this.autoIncrementable = autoIncrementable;
		}
		
		@Override
		public String toString() {
			return "TypeInfo{" +
					"name='" + name + '\'' +
					", type=" + type +
					", precision=" + precision +
					", literalPrefix='" + literalPrefix + '\'' +
					", literalSuffix='" + literalSuffix + '\'' +
					", createParams='" + createParams + '\'' +
					", nullable=" + nullable +
					", caseSensitive=" + caseSensitive +
					", autoIncrementable=" + autoIncrementable +
					'}';
		}
		
	}
}
