package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.codefilarete.jumper.schema.DDLElement.ColumnMetadata;
import org.codefilarete.jumper.schema.DDLElement.ForeignKeyMetadata;
import org.codefilarete.jumper.schema.DDLElement.IndexMetadata;
import org.codefilarete.jumper.schema.DDLElement.PrimaryKeyMetadata;
import org.codefilarete.jumper.schema.DDLElement.ProcedureMetadata;
import org.codefilarete.jumper.schema.DDLElement.SequenceMetadata;
import org.codefilarete.jumper.schema.DDLElement.TableMetadata;
import org.codefilarete.jumper.schema.DDLElement.ViewMetadata;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Nullable;

public class MetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public MetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}
	
	public SortedSet<ColumnMetadata> giveColumns(String catalog, String schema, String tablePattern) {
		try (ResultSet tableResultSet = metaData.getColumns(catalog, schema, tablePattern, "%")) {
			ResultSetIterator<ColumnMetadata> resultSetIterator = new ResultSetIterator<ColumnMetadata>(tableResultSet) {
				@Override
				public ColumnMetadata convert(ResultSet resultSet) {
					ColumnMetadata result = new ColumnMetadata(
							ColumnMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							ColumnMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							ColumnMetaDataPseudoTable.INSTANCE.tableName.giveValue(resultSet)
					);
					ColumnMetaDataPseudoTable.INSTANCE.columnName.apply(resultSet, result::setName);
					ColumnMetaDataPseudoTable.INSTANCE.type.apply(resultSet, result::setSqlType);
					ColumnMetaDataPseudoTable.INSTANCE.typeName.apply(resultSet, result::setVendorType);
					ColumnMetaDataPseudoTable.INSTANCE.size.apply(resultSet, result::setSize);
					ColumnMetaDataPseudoTable.INSTANCE.decimalDigits.apply(resultSet, result::setPrecision);
					ColumnMetaDataPseudoTable.INSTANCE.nullable.apply(resultSet, result::setNullable);
					ColumnMetaDataPseudoTable.INSTANCE.isAutoIncrement.apply(resultSet, result::setAutoIncrement);
					ColumnMetaDataPseudoTable.INSTANCE.ordinalPosition.apply(resultSet, result::setPosition);
					return result;
				}
			};
			SortedSet<ColumnMetadata> result = new TreeSet<>(Comparator.comparing(ColumnMetadata::getPosition));
			result.addAll(resultSetIterator.convert());
			return result;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Set<SequenceMetadata> giveSequences(String catalog, String schema) {
		String schemaCriteria = null;
		if (schema != null) {
			if (schema.contains("%")) {
				schemaCriteria = "like '" + schema + "'";
			} else {
				schemaCriteria = " = '" + schema + "'";
			}
		}
		if (schemaCriteria != null) {
			schemaCriteria = "SEQUENCE_SCHEMA " + schemaCriteria;
		}
		String sequenceSql = "SELECT SEQUENCE_NAME, SEQUENCE_SCHEMA, SEQUENCE_CATALOG FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES";
		if (schemaCriteria != null) {
			sequenceSql += " WHERE " + schemaCriteria;
		}
		try (PreparedStatement selectSequenceStatement = metaData.getConnection().prepareStatement(sequenceSql);
			 ResultSet tableResultSet = selectSequenceStatement.executeQuery()) {
			ResultSetIterator<SequenceMetadata> resultSetIterator = new ResultSetIterator<SequenceMetadata>(tableResultSet) {
				@Override
				public SequenceMetadata convert(ResultSet resultSet) {
					return new SequenceMetadata(
							SequenceMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							SequenceMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							SequenceMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet)
					);
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Gives foreign keys exported by tables matching given pattern
	 *
	 * @param catalog table catalog
	 * @param schema table schema
	 * @param tablePattern table pattern for which foreign keys must be retrieved
	 * @return foreign keys exported by tables matching given pattern
	 */
	public Set<ForeignKeyMetadata> giveExportedKeys(String catalog, String schema, String tablePattern) {
		try (ResultSet tableResultSet = metaData.getExportedKeys(catalog, schema, tablePattern)) {
			Map<String, ForeignKeyMetadata> cache = new HashMap<>();
			ResultSetIterator<ForeignKeyMetadata> resultSetIterator = new ResultSetIterator<ForeignKeyMetadata>(tableResultSet) {
				@Override
				public ForeignKeyMetadata convert(ResultSet resultSet) {
					String name = ExportedKeysMetaDataPseudoTable.INSTANCE.fkName.giveValue(resultSet);
					ForeignKeyMetadata result = cache.computeIfAbsent(name, k ->
							new ForeignKeyMetadata(
									k,
									ExportedKeysMetaDataPseudoTable.INSTANCE.fkCatalog.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.fkSchema.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.fkTableName.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.pkCatalog.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.pkSchema.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.pkTableName.giveValue(resultSet)
							));
					result.addColumn(
							ExportedKeysMetaDataPseudoTable.INSTANCE.fkColumnName.giveValue(resultSet),
							ExportedKeysMetaDataPseudoTable.INSTANCE.pkColumnName.giveValue(resultSet)
					);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Gives foreign keys pointing to primary of tables matching given pattern
	 *
	 * @param catalog table catalog
	 * @param schema table schema
	 * @param tablePattern table pattern which primary keys are target of foreign keys
	 * @return foreign keys pointing to primary of tables matching given pattern
	 */
	public Set<ForeignKeyMetadata> giveImportedKeys(String catalog, String schema, String tablePattern) {
		try (ResultSet tableResultSet = metaData.getImportedKeys(catalog, schema, tablePattern)) {
			Map<String, ForeignKeyMetadata> cache = new HashMap<>();
			ResultSetIterator<ForeignKeyMetadata> resultSetIterator = new ResultSetIterator<ForeignKeyMetadata>(tableResultSet) {
				@Override
				public ForeignKeyMetadata convert(ResultSet resultSet) {
					String name = ExportedKeysMetaDataPseudoTable.INSTANCE.fkName.giveValue(resultSet);
					ForeignKeyMetadata result = cache.computeIfAbsent(name, k ->
							new ForeignKeyMetadata(
									k,
									ExportedKeysMetaDataPseudoTable.INSTANCE.fkCatalog.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.fkSchema.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.fkTableName.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.pkCatalog.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.pkSchema.giveValue(resultSet),
									ExportedKeysMetaDataPseudoTable.INSTANCE.pkTableName.giveValue(resultSet)
							));
					result.addColumn(
							ExportedKeysMetaDataPseudoTable.INSTANCE.fkColumnName.giveValue(resultSet),
							ExportedKeysMetaDataPseudoTable.INSTANCE.pkColumnName.giveValue(resultSet)
					);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public PrimaryKeyMetadata givePrimaryKey(String catalog, String schema, String tableName) {
		try (ResultSet tableResultSet = metaData.getPrimaryKeys(catalog, schema, tableName)) {
			Nullable<PrimaryKeyMetadata> result = Nullable.nullable((PrimaryKeyMetadata) null);
			ResultSetIterator<PrimaryKeyMetadata> resultSetIterator = new ResultSetIterator<PrimaryKeyMetadata>(tableResultSet) {
				@Override
				public PrimaryKeyMetadata convert(ResultSet resultSet) {
					result.elseSet(new PrimaryKeyMetadata(
							PrimaryKeysMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet), PrimaryKeysMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet), PrimaryKeysMetaDataPseudoTable.INSTANCE.tableName.giveValue(resultSet), PrimaryKeysMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet)
					));
					result.get().addColumn(
							PrimaryKeysMetaDataPseudoTable.INSTANCE.columnName.giveValue(resultSet)
					);
					return result.get();
				}
			};
			resultSetIterator.convert();
			return result.get();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Set<TypeInfo> giveColumnTypes() {
		try (ResultSet tableResultSet = metaData.getTypeInfo()) {
			ResultSetIterator<TypeInfo> resultSetIterator = new ResultSetIterator<TypeInfo>(tableResultSet) {
				@Override
				public TypeInfo convert(ResultSet resultSet) {
					TypeInfo result = new TypeInfo(
							TypeInfoMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet),
							TypeInfoMetaDataPseudoTable.INSTANCE.type.giveValue(resultSet));
					TypeInfoMetaDataPseudoTable.INSTANCE.createParams.apply(resultSet, result::setCreateParams);
					TypeInfoMetaDataPseudoTable.INSTANCE.literalPrefix.apply(resultSet, result::setLiteralPrefix);
					TypeInfoMetaDataPseudoTable.INSTANCE.literalSuffix.apply(resultSet, result::setLiteralSuffix);
					TypeInfoMetaDataPseudoTable.INSTANCE.precision.apply(resultSet, result::setPrecision);
					TypeInfoMetaDataPseudoTable.INSTANCE.nullable.apply(resultSet, result::setNullable);
					TypeInfoMetaDataPseudoTable.INSTANCE.autoIncrementable.apply(resultSet, result::setAutoIncrementable);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern) {
		try (ResultSet tableResultSet = metaData.getTables(catalog, schema, tableNamePattern, new String[] { "TABLE" })) {
			ResultSetIterator<TableMetadata> resultSetIterator = new ResultSetIterator<TableMetadata>(tableResultSet) {
				@Override
				public TableMetadata convert(ResultSet resultSet) {
					TableMetadata result = new TableMetadata(
							TableMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							TableMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet)
					);
					TableMetaDataPseudoTable.INSTANCE.tableName.apply(resultSet, result::setName);
					TableMetaDataPseudoTable.INSTANCE.remarks.apply(resultSet, result::setRemarks);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Set<ViewMetadata> giveViews(String catalog, String schema, String tableNamePattern) {
		try (ResultSet tableResultSet = metaData.getTables(catalog, schema, tableNamePattern, new String[] { "VIEW" })) {
			ResultSetIterator<ViewMetadata> resultSetIterator = new ResultSetIterator<ViewMetadata>(tableResultSet) {
				@Override
				public ViewMetadata convert(ResultSet resultSet) {
					ViewMetadata result = new ViewMetadata(
							TableMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							TableMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet)
					);
					TableMetaDataPseudoTable.INSTANCE.tableName.apply(resultSet, result::setName);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Set<IndexMetadata> giveIndexes(String catalog, String schema, String tableName) {
		try (ResultSet tableResultSet = metaData.getIndexInfo(catalog, schema, tableName, false, false)) {
			Map<String, IndexMetadata> cache = new HashMap<>();
			ResultSetIterator<IndexMetadata> resultSetIterator = new ResultSetIterator<IndexMetadata>(tableResultSet) {
				@Override
				public IndexMetadata convert(ResultSet resultSet) {
					String name = IndexMetaDataPseudoTable.INSTANCE.indexName.giveValue(resultSet);
					IndexMetadata result = cache.computeIfAbsent(name, k -> {
						IndexMetadata newInstance = new IndexMetadata(
								IndexMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
								IndexMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
								IndexMetaDataPseudoTable.INSTANCE.tableName.giveValue(resultSet)
						);
						IndexMetaDataPseudoTable.INSTANCE.indexName.apply(resultSet, newInstance::setName);
						IndexMetaDataPseudoTable.INSTANCE.nonUnique.apply(resultSet, nonUnique -> newInstance.setUnique(!nonUnique));
						return newInstance;
					});
					String a_d = IndexMetaDataPseudoTable.INSTANCE.ascOrDesc.giveValue(resultSet);
					Boolean ascOrDesc;
					switch (a_d) {
						case "A":
							ascOrDesc = true; break;
						case "D":
							ascOrDesc = false; break;
						default:
							ascOrDesc = null; break;
					}
					result.addColumn(
							IndexMetaDataPseudoTable.INSTANCE.columnName.giveValue(resultSet),
							ascOrDesc
					);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Set<ProcedureMetadata> giveProcedures(String catalog, String schema, String procedurePatternName) {
		try (ResultSet tableResultSet = metaData.getProcedures(catalog, schema, procedurePatternName)) {
			Map<String, ProcedureMetadata> cache = new HashMap<>();
			ResultSetIterator<ProcedureMetadata> resultSetIterator = new ResultSetIterator<ProcedureMetadata>(tableResultSet) {
				@Override
				public ProcedureMetadata convert(ResultSet resultSet) {
					ProcedureMetadata newInstance = new ProcedureMetadata(
							ProcedureMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.remarks.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.procedureType.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.specificName.giveValue(resultSet)
					);
					return newInstance;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
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
	
	
	static class ProcedureMetaDataPseudoTable extends Table<ProcedureMetaDataPseudoTable> {
		
		static final ProcedureMetaDataPseudoTable INSTANCE = new ProcedureMetaDataPseudoTable();
		
		/** procedure catalog (may be null) */
		private final ColumnReader<String> catalog = new ColumnReader<>(addColumn("PROCEDURE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** procedure schema (may be null) */
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("PROCEDURE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** procedure name */
		private final ColumnReader<String> name = new ColumnReader<>(addColumn("PROCEDURE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** explanatory comment on the procedure */
		private final ColumnReader<String> remarks = new ColumnReader<>(addColumn("REMARKS", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * Kind of procedure:
		 * - {@link java.sql.DatabaseMetaData#procedureResultUnknown} : Cannot determine if a return value will be returned
		 * - {@link java.sql.DatabaseMetaData#procedureNoResult} : Does not return a return value
		 * - {@link java.sql.DatabaseMetaData#procedureReturnsResult} : Returns a return value
		 */
		private final ColumnReader<Short> procedureType = new ColumnReader<>(addColumn("PROCEDURE_TYPE", short.class), ResultSet::getShort);
		
		/** The name which uniquely identifies this procedure within its schema */
		private final ColumnReader<String> specificName = new ColumnReader<>(addColumn("SPECIFIC_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		public ProcedureMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getProcedures() ResultSet
			super("ProcedureMetaData");
		}
	}
	
	static class IndexMetaDataPseudoTable extends Table<IndexMetaDataPseudoTable> {
		
		static final IndexMetaDataPseudoTable INSTANCE = new IndexMetaDataPseudoTable();
		
		/** table catalog (may be null) */
		private final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table schema (may be null) */
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table name */
		private final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic */
		private final ColumnReader<Boolean> nonUnique = new ColumnReader<>(addColumn("NON_UNIQUE", boolean.class), DefaultResultSetReaders.BOOLEAN_PRIMITIVE_READER);
		
		/** index catalog (may be null); null when TYPE is tableIndexStatistic */
		private final ColumnReader<String> indexQualifier = new ColumnReader<>(addColumn("INDEX_QUALIFIER", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** index name; null when TYPE is tableIndexStatistic */
		private final ColumnReader<String> indexName = new ColumnReader<>(addColumn("INDEX_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * TYPE short => index type:
		 * - {@link DatabaseMetaData#tableIndexStatistic} : this identifies table statistics that are returned in conjunction with a table's index descriptions
		 * - {@link DatabaseMetaData#tableIndexClustered} : this is a clustered index
		 * - {@link DatabaseMetaData#tableIndexHashed} : this is a hashed index
		 * - {@link DatabaseMetaData#tableIndexOther} : this is some other style of index
		 */
		private final ColumnReader<Short> type = new ColumnReader<>(addColumn("TYPE", short.class), ResultSet::getShort);

		/** column sequence number within index; zero when TYPE is tableIndexStatistic */
		private final ColumnReader<Short> ordinalPosition = new ColumnReader<>(addColumn("ORDINAL_POSITION", short.class), ResultSet::getShort);
		
		/** column name; null when TYPE is tableIndexStatistic */
		private final ColumnReader<String> columnName = new ColumnReader<>(addColumn("COLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);

		/** column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic */
		private final ColumnReader<String> ascOrDesc = new ColumnReader<>(addColumn("ASC_OR_DESC", String.class), DefaultResultSetReaders.STRING_READER);

		/** When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index. */
		private final ColumnReader<Long> cardinality = new ColumnReader<>(addColumn("CARDINALITY", long.class), DefaultResultSetReaders.LONG_READER);
		
		/** When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index. */
		private final ColumnReader<Long> pages = new ColumnReader<>(addColumn("PAGES", long.class), DefaultResultSetReaders.LONG_READER);
		
		/** Filter condition, if any. (may be null) */
		private final ColumnReader<String> filterCondition = new ColumnReader<>(addColumn("FILTER_CONDITION", String.class), DefaultResultSetReaders.STRING_READER);
		
		public IndexMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getIndexInfo() ResultSet
			super("IndexMetaData");
		}
	}
	
	static class TableMetaDataPseudoTable extends Table<TableMetaDataPseudoTable> {
		
		static final TableMetaDataPseudoTable INSTANCE = new TableMetaDataPseudoTable();
		
		private enum TableType {
			TABLE,
			VIEW,
			SYSTEM_TABLE,
			GLOBAL_TEMPORARY,
			LOCAL_TEMPORARY,
			ALIAS,
			SYNONYM;
		}
		
		private enum Generation {
			SYSTEM,
			USER,
			DERIVED;
		}
		
		/** table catalog (may be null) */
		private final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table schema (may be null) */
		private final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** TABLE_NAME String => table name */
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> tableType = new ColumnReader<>(addColumn("TABLE_TYPE", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * Explanatory comment on the table
		 */
		private final ColumnReader<String> remarks = new ColumnReader<>(addColumn("REMARKS", String.class), DefaultResultSetReaders.STRING_READER);
		/**
		 * Types catalog
		 */
		private final ColumnReader<String> catalogType = new ColumnReader<>(addColumn("TYPE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		/**
		 * Types schema
		 */
		private final ColumnReader<String> schemaType = new ColumnReader<>(addColumn("TYPE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		private final ColumnReader<String> typeName = new ColumnReader<>(addColumn("TYPE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		/**
		 * Name of the designated "identifier" column of a typed table
		 */
		private final ColumnReader<String> selfReferencingColName = new ColumnReader<>(addColumn("SELF_REFERENCING_COL_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		/**
		 * Specifies how values in SELF_REFERENCING_COL_NAME are created
		 */
		private final ColumnReader<Generation> refGeneration = new ColumnReader<>(addColumn("REF_GENERATION", Generation.class),
				(resultSet, columnName) -> Generation.valueOf(DefaultResultSetReaders.STRING_READER.get(resultSet, columnName)));
		
		public TableMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getTables() ResultSet
			super("TableMetadata");
		}
	}
	
	static class TypeInfoMetaDataPseudoTable extends Table<TypeInfoMetaDataPseudoTable> {
		
		static final TypeInfoMetaDataPseudoTable INSTANCE = new TypeInfoMetaDataPseudoTable();

//		TYPE_NAME String => Type name
//		DATA_TYPE int => SQL data type from java.sql.Types
//		PRECISION int => maximum precision
//		LITERAL_PREFIX String => prefix used to quote a literal (may be null)
//		LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
//		CREATE_PARAMS String => parameters used in creating the type (may be null)
//		NULLABLE short => can you use NULL for this type
//				typeNoNulls - does not allow NULL values
//				typeNullable - allows NULL values
//				typeNullableUnknown - nullability unknown
//		CASE_SENSITIVE boolean=> is it case sensitive.
//		SEARCHABLE short => can you use "WHERE" based on this type:
//				typePredNone - No support
//				typePredChar - Only supported with WHERE .. LIKE
//				typePredBasic - Supported except for WHERE .. LIKE
//				typeSearchable - Supported for all WHERE ..
//		UNSIGNED_ATTRIBUTE boolean => is it unsigned.
//		FIXED_PREC_SCALE boolean => can it be a money value.
//		AUTO_INCREMENT boolean => can it be used for an auto-increment value.
//		LOCAL_TYPE_NAME String => localized version of type name (may be null)
//		MINIMUM_SCALE short => minimum scale supported
//		MAXIMUM_SCALE short => maximum scale supported
//		SQL_DATA_TYPE int => unused
//		SQL_DATETIME_SUB int => unused
//		NUM_PREC_RADIX int => usually 2 or 10
		
		/** Type name */
		private final ColumnReader<String> name = new ColumnReader<>(addColumn("TYPE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** SQL data type from {@link java.sql.Types} */
		private final ColumnReader<Integer> type = new ColumnReader<>(addColumn("DATA_TYPE", int.class), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		
		/** maximum precision */
		private final ColumnReader<Integer> precision = new ColumnReader<>(addColumn("PRECISION", int.class), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		
		/** prefix used to quote a literal (may be null) */
		private final ColumnReader<String> literalPrefix = new ColumnReader<>(addColumn("LITERAL_PREFIX", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** suffix used to quote a literal (may be null) */
		private final ColumnReader<String> literalSuffix = new ColumnReader<>(addColumn("LITERAL_SUFFIX", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** parameters used in creating the type (may be null) */
		private final ColumnReader<String> createParams = new ColumnReader<>(addColumn("CREATE_PARAMS", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * an you use NULL for this type, possible values :
		 * - {@link DatabaseMetaData#typeNoNulls}
		 * - {@link DatabaseMetaData#typeNullable}
		 * - {@link DatabaseMetaData#typeNullableUnknown}
		 */
		private final ColumnReader<Boolean> nullable = new ColumnReader<>(addColumn("NULLABLE", boolean.class), (rs, col) -> rs.getShort(col) == DatabaseMetaData.typeNullable);
		
		/** can it be used for an auto-increment value */
		private final ColumnReader<Boolean> autoIncrementable = new ColumnReader<>(addColumn("AUTO_INCREMENT", boolean.class), DefaultResultSetReaders.BOOLEAN_PRIMITIVE_READER);
		
		public TypeInfoMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getTypeInfo() ResultSet
			super("TypeInfoMetaData");
		}
	}
	
	static class ExportedKeysMetaDataPseudoTable extends Table<ExportedKeysMetaDataPseudoTable> {
		
		// PKTABLE_CAT String => parent key table catalog (may be null)
		// PKTABLE_SCHEM String => parent key table schema (may be null)
		// PKTABLE_NAME String => parent key table name
		// PKCOLUMN_NAME String => parent key column name
		// FKTABLE_CAT String => foreign key table catalog (may be null) being exported (may be null)
		// FKTABLE_SCHEM String => foreign key table schema (may be null) being exported (may be null)
		// FKTABLE_NAME String => foreign key table name being exported
		// FKCOLUMN_NAME String => foreign key column name being exported
		// KEY_SEQ short => sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
		// UPDATE_RULE short => What happens to foreign key when parent key is updated:
		// 	importedNoAction - do not allow update of parent key if it has been imported
		// 	importedKeyCascade - change imported key to agree with parent key update
		// 	importedKeySetNull - change imported key to NULL if its parent key has been updated
		// 	importedKeySetDefault - change imported key to default values if its parent key has been updated
		// 	importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
		// DELETE_RULE short => What happens to the foreign key when parent key is deleted.
		// 	importedKeyNoAction - do not allow delete of parent key if it has been imported
		// 	importedKeyCascade - delete rows that import a deleted key
		// 	importedKeySetNull - change imported key to NULL if its primary key has been deleted
		// 	importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
		// 	importedKeySetDefault - change imported key to default if its parent key has been deleted
		// FK_NAME String => foreign key name (may be null)
		// PK_NAME String => parent key name (may be null)
		// DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
		// 	importedKeyInitiallyDeferred - see SQL92 for definition
		// 	importedKeyInitiallyImmediate - see SQL92 for definition
		// 	importedKeyNotDeferrable - see SQL92 for definition
		
		static final ExportedKeysMetaDataPseudoTable INSTANCE = new ExportedKeysMetaDataPseudoTable();
		
		private final ColumnReader<String> pkCatalog = new ColumnReader<>(addColumn("PKTABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> pkSchema = new ColumnReader<>(addColumn("PKTABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> pkTableName = new ColumnReader<>(addColumn("PKTABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> pkColumnName = new ColumnReader<>(addColumn("PKCOLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> fkCatalog = new ColumnReader<>(addColumn("FKTABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> fkSchema = new ColumnReader<>(addColumn("FKTABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> fkTableName = new ColumnReader<>(addColumn("FKTABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> fkColumnName = new ColumnReader<>(addColumn("FKCOLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> fkName = new ColumnReader<>(addColumn("FK_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> pkName = new ColumnReader<>(addColumn("PK_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		public ExportedKeysMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getFColumns() ResultSet
			super("ForeignKeyMetaData");
		}
	}
	
	static class PrimaryKeysMetaDataPseudoTable extends Table<PrimaryKeysMetaDataPseudoTable> {
		
		static final PrimaryKeysMetaDataPseudoTable INSTANCE = new PrimaryKeysMetaDataPseudoTable();
		
		/** table catalog (may be null) */
		private final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table schema (may be null) */
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table name */
		private final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** column name */
		private final ColumnReader<String> columnName = new ColumnReader<>(addColumn("COLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * sequence number within primary key( a value of 1 represents the first column of the primary key,
		 * a value of 2 would represent the second column within the primary key).
		 */
		private final ColumnReader<String> columnIndex = new ColumnReader<>(addColumn("KEY_SEQ", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** primary key name (may be null) */
		private final ColumnReader<String> name = new ColumnReader<>(addColumn("PK_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		
		public PrimaryKeysMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getExportedKeys() ResultSet
			super("ExportedKeyMetaData");
		}
	}
	
	static class SequenceMetaDataPseudoTable extends Table<SequenceMetaDataPseudoTable> {
		
		static final SequenceMetaDataPseudoTable INSTANCE = new SequenceMetaDataPseudoTable();
		
		private final ColumnReader<String> catalog = new ColumnReader<>(addColumn("SEQUENCE_CATALOG", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("SEQUENCE_SCHEMA", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> name = new ColumnReader<>(addColumn("SEQUENCE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		public SequenceMetaDataPseudoTable() {
			// This table has no real name, it's made to map query on information_schema to retrieve views
			super("SequenceMetaData");
		}
	}
	
	static class ColumnMetaDataPseudoTable extends Table<ColumnMetaDataPseudoTable> {
		
		static final ColumnMetaDataPseudoTable INSTANCE = new ColumnMetaDataPseudoTable();
		
		private final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> columnName = new ColumnReader<>(addColumn("COLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** SQL type from {@link java.sql.Types} */
		private final ColumnReader<Integer> type = new ColumnReader<>(addColumn("DATA_TYPE", int.class), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		
		/** Data source dependent type name, for a User-Defined-Type the type name is fully qualified */
		private final ColumnReader<String> typeName = new ColumnReader<>(addColumn("TYPE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * Specifies the column size for the given column.
		 * - For numeric data, this is the maximum precision.
		 * - For character data, this is the length in characters.
		 * - For datetime datatypes, this is the length in characters of the String representation (assuming the maximum
		 * allowed precision of the fractional seconds component).
		 * - For binary data, this is the length in bytes.
		 * - For the ROWID datatype, this is the length in bytes.
		 * - Null is returned for data types where the column size is not applicable.
		 */
		private final ColumnReader<Integer> size = new ColumnReader<>(addColumn("COLUMN_SIZE", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** not used */
		private final ColumnReader<Object> bufferLength = new ColumnReader<>(addColumn("BUFFER_LENGTH", Object.class), ResultSet::getObject);
		
		/** the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable. */
		private final ColumnReader<Integer> decimalDigits = new ColumnReader<>(addColumn("DECIMAL_DIGITS", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** Radix (typically either 10 or 2) */
		private final ColumnReader<Integer> radix = new ColumnReader<>(addColumn("NUM_PREC_RADIX", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/**
		 * Is null allowed, possible values
		 * {@link DatabaseMetaData#columnNoNulls}
		 * {@link DatabaseMetaData#columnNullable}
		 * {@link DatabaseMetaData#columnNullableUnknown}
		 */
		private final ColumnReader<Boolean> nullable = new ColumnReader<>(addColumn("NULLABLE", boolean.class), (rs, col) -> rs.getInt(col) == DatabaseMetaData.columnNullable);
		
		/** comment describing column (may be null) */
		private final ColumnReader<String> remarks = new ColumnReader<>(addColumn("REMARKS", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null) */
		private final ColumnReader<String> defaultValue = new ColumnReader<>(addColumn("COLUMN_DEF", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** unused */
		private final ColumnReader<Integer> sqlDataType = new ColumnReader<>(addColumn("SQL_DATA_TYPE", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** unused */
		private final ColumnReader<Integer> sqlDatetimeSub = new ColumnReader<>(addColumn("SQL_DATETIME_SUB", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** for char types the maximum number of bytes in the column */
		private final ColumnReader<Integer> charOctetLength = new ColumnReader<>(addColumn("CHAR_OCTET_LENGTH", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/**
		 * index of column in table (starting at 1)
		 */
		private final ColumnReader<Integer> ordinalPosition = new ColumnReader<>(addColumn("ORDINAL_POSITION", Integer.class), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		
		/**
		 * ISO rules are used to determine the nullability for a column, possible values
		 * - YES : if the column can include NULLs
		 * - NO : if the column cannot include NULLs
		 * - empty string : if the nullability for the column is unknown
		 */
		private final ColumnReader<String> isNullable = new ColumnReader<>(addColumn("IS_NULLABLE", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF) */
		private final ColumnReader<String> scopeCatalog = new ColumnReader<>(addColumn("SCOPE_CATALOG", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF) */
		private final ColumnReader<String> scopeSchema = new ColumnReader<>(addColumn("SCOPE_SCHEMA", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF) */
		private final ColumnReader<String> scopeTable = new ColumnReader<>(addColumn("SCOPE_TABLE", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF) */
		private final ColumnReader<Short> sourceDataType = new ColumnReader<>(addColumn("SOURCE_DATA_TYPE", Short.class), ResultSet::getShort);
		
		/**
		 * Indicates whether this column is auto incremented, possible values
		 * - YES : if the column is auto incremented
		 * - NO : if the column is not auto incremented
		 * - empty string : if it cannot be determined whether the column is auto incremented
		 */
		private final ColumnReader<Boolean> isAutoIncrement = new ColumnReader<>(addColumn("IS_AUTOINCREMENT", boolean.class), (rs, col) -> "yes".equalsIgnoreCase(rs.getString(col)));
		
		/**
		 * Indicates whether this is a generated column
		 * - YES : if this a generated column
		 * - NO : if this not a generated column
		 * - empty string : if it cannot be determined whether this is a generated column
		 */
		private final ColumnReader<String> isGeneratedColumn = new ColumnReader<>(addColumn("IS_GENERATEDCOLUMN", String.class), DefaultResultSetReaders.STRING_READER);
		
		public ColumnMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getColumns() ResultSet
			super("ColumnMetaData");
		}
	}
	
	static class ColumnReader<T> {
		
		private final Column<?, T> column;
		private final ResultSetReader<T> reader;
		
		ColumnReader(Column<?, T> column, ResultSetReader<T> reader) {
			this.column = column;
			this.reader = reader;
		}
		
		void apply(ResultSet row, Consumer<T> consumer) {
			consumer.accept(giveValue(row));
		}
		
		T giveValue(ResultSet row) {
			return reader.get(row, column.getName());
		}
	}
}
