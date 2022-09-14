package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
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

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.bean.Objects;

public class DefaultMetadataReader implements MetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public DefaultMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}
	
	@Override
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
	
	@Override
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
	
	@Override
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
	
	@Override
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
	
	@Override
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
	
	@Override
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
	
	@Override
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
					result.addColumn(
							IndexMetaDataPseudoTable.INSTANCE.columnName.giveValue(resultSet),
							IndexMetaDataPseudoTable.INSTANCE.ascOrDesc.giveValue(resultSet)
					);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Set<ProcedureMetadata> giveProcedures(String catalog, String schema, String procedurePatternName) {
		try (ResultSet tableResultSet = metaData.getProcedures(catalog, schema, procedurePatternName)) {
			ResultSetIterator<ProcedureMetadata> resultSetIterator = new ResultSetIterator<ProcedureMetadata>(tableResultSet) {
				@Override
				public ProcedureMetadata convert(ResultSet resultSet) {
					return new ProcedureMetadata(
							ProcedureMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.remarks.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.procedureType.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.specificName.giveValue(resultSet)
					);
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getProcedures(String, String, String)}
	 * @author Guillaume Mary
	 */
	public static class ProcedureMetaDataPseudoTable extends Table<ProcedureMetaDataPseudoTable> {
		
		public static final ProcedureMetaDataPseudoTable INSTANCE = new ProcedureMetaDataPseudoTable();
		
		/** Procedure catalog (may be null) */
		public final ColumnReader<String> catalog = new ColumnReader<>(addColumn("PROCEDURE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Procedure schema (may be null) */
		public final ColumnReader<String> schema = new ColumnReader<>(addColumn("PROCEDURE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Procedure name */
		public final ColumnReader<String> name = new ColumnReader<>(addColumn("PROCEDURE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Explanatory comment on the procedure */
		public final ColumnReader<String> remarks = new ColumnReader<>(addColumn("REMARKS", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * Kind of procedure:
		 * - {@link DatabaseMetaData#procedureResultUnknown} : Cannot determine if a return value will be returned
		 * - {@link DatabaseMetaData#procedureNoResult} : Does not return a return value
		 * - {@link DatabaseMetaData#procedureReturnsResult} : Returns a return value
		 */
		public final ColumnReader<Short> procedureType = new ColumnReader<>(addColumn("PROCEDURE_TYPE", short.class), ResultSet::getShort);
		
		/** The name which uniquely identifies this procedure within its schema */
		public final ColumnReader<String> specificName = new ColumnReader<>(addColumn("SPECIFIC_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		public ProcedureMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getProcedures() ResultSet
			super("ProcedureMetaData");
		}
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}
	 * @author Guillaume Mary
	 */
	public static class IndexMetaDataPseudoTable extends Table<IndexMetaDataPseudoTable> {
		
		public static final IndexMetaDataPseudoTable INSTANCE = new IndexMetaDataPseudoTable();
		
		/** Table catalog (may be null) */
		public final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Table schema (may be null) */
		public final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Table name */
		public final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Can index values be non-unique. false when TYPE is tableIndexStatistic */
		public final ColumnReader<Boolean> nonUnique = new ColumnReader<>(addColumn("NON_UNIQUE", boolean.class), DefaultResultSetReaders.BOOLEAN_PRIMITIVE_READER);
		
		/** Index catalog (may be null); null when TYPE is tableIndexStatistic */
		public final ColumnReader<String> indexQualifier = new ColumnReader<>(addColumn("INDEX_QUALIFIER", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Index name; null when TYPE is tableIndexStatistic */
		public final ColumnReader<String> indexName = new ColumnReader<>(addColumn("INDEX_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * Index type:
		 * - {@link DatabaseMetaData#tableIndexStatistic} : this identifies table statistics that are returned in conjunction with a table's index descriptions
		 * - {@link DatabaseMetaData#tableIndexClustered} : this is a clustered index
		 * - {@link DatabaseMetaData#tableIndexHashed} : this is a hashed index
		 * - {@link DatabaseMetaData#tableIndexOther} : this is some other style of index
		 */
		public final ColumnReader<Short> type = new ColumnReader<>(addColumn("TYPE", short.class), ResultSet::getShort);

		/** Column sequence number within index; zero when TYPE is tableIndexStatistic */
		public final ColumnReader<Short> ordinalPosition = new ColumnReader<>(addColumn("ORDINAL_POSITION", short.class), ResultSet::getShort);
		
		/** Column name; null when TYPE is tableIndexStatistic */
		public final ColumnReader<String> columnName = new ColumnReader<>(addColumn("COLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);

		/**
		 * Column sort sequence, originally :
		 * - "A" => ascending
		 * - "D" => descending
		 * - may be null if sort sequence is not supported
		 * - null when TYPE is {@link DatabaseMetaData#tableIndexStatistic}
		 * Transformed as Boolean :
		 * - true for ascending
		 * - false for descending
		 * - null for original null cases
		 */
		public final ColumnReader<Boolean> ascOrDesc = new ColumnReader<>(addColumn("ASC_OR_DESC", Boolean.class), (rs, col) -> {
			switch (Objects.preventNull(rs.getString(col))) {
				case "A":
					return true;
				case "D":
					return false;
				default:
					return null;
			}
		});

		/** When TYPE is {@link DatabaseMetaData#tableIndexStatistic}, then this is the number of rows in the table; otherwise, it is the number of unique values in the index. */
		public final ColumnReader<Long> cardinality = new ColumnReader<>(addColumn("CARDINALITY", long.class), DefaultResultSetReaders.LONG_READER);
		
		/** When TYPE is {@link DatabaseMetaData#tableIndexStatistic}, then this is the number of pages used for the table, otherwise it is the number of pages used for the current index. */
		public final ColumnReader<Long> pages = new ColumnReader<>(addColumn("PAGES", long.class), DefaultResultSetReaders.LONG_READER);
		
		/** Filter condition, if any. (may be null) */
		public final ColumnReader<String> filterCondition = new ColumnReader<>(addColumn("FILTER_CONDITION", String.class), DefaultResultSetReaders.STRING_READER);
		
		public IndexMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getIndexInfo() ResultSet
			super("IndexMetaData");
		}
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getTables(String, String, String, String[])}
	 * @author Guillaume Mary
	 */
	public static class TableMetaDataPseudoTable extends Table<TableMetaDataPseudoTable> {
		
		public static final TableMetaDataPseudoTable INSTANCE = new TableMetaDataPseudoTable();
		
		public enum Generation {
			SYSTEM,
			USER,
			DERIVED;
		}
		
		/** Table catalog (may be null) */
		public final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Table schema (may be null) */
		public final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Table name */
		public final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM". */
		public final ColumnReader<String> tableType = new ColumnReader<>(addColumn("TABLE_TYPE", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Explanatory comment on the table */
		public final ColumnReader<String> remarks = new ColumnReader<>(addColumn("REMARKS", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Types catalog */
		public final ColumnReader<String> catalogType = new ColumnReader<>(addColumn("TYPE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Types schema */
		public final ColumnReader<String> schemaType = new ColumnReader<>(addColumn("TYPE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Type name (may be null) */
		public final ColumnReader<String> typeName = new ColumnReader<>(addColumn("TYPE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Name of the designated "identifier" column of a typed table */
		public final ColumnReader<String> selfReferencingColName = new ColumnReader<>(addColumn("SELF_REFERENCING_COL_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Specifies how values in SELF_REFERENCING_COL_NAME are created */
		public final ColumnReader<Generation> refGeneration = new ColumnReader<>(addColumn("REF_GENERATION", Generation.class),
				(resultSet, columnName) -> Generation.valueOf(DefaultResultSetReaders.STRING_READER.get(resultSet, columnName)));
		
		public TableMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getTables() ResultSet
			super("TableMetadata");
		}
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getExportedKeys(String, String, String)}
	 * @author Guillaume Mary
	 */
	public static class ExportedKeysMetaDataPseudoTable extends Table<ExportedKeysMetaDataPseudoTable> {
		
		public static final ExportedKeysMetaDataPseudoTable INSTANCE = new ExportedKeysMetaDataPseudoTable();
		
		/** Parent key table catalog (may be null) */
		public final ColumnReader<String> pkCatalog = new ColumnReader<>(addColumn("PKTABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Parent key table schema (may be null) */
		public final ColumnReader<String> pkSchema = new ColumnReader<>(addColumn("PKTABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Parent key table name */
		public final ColumnReader<String> pkTableName = new ColumnReader<>(addColumn("PKTABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Parent key column name */
		public final ColumnReader<String> pkColumnName = new ColumnReader<>(addColumn("PKCOLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Foreign key table catalog (may be null) being exported (may be null) */
		public final ColumnReader<String> fkCatalog = new ColumnReader<>(addColumn("FKTABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Foreign key table schema (may be null) being exported (may be null) */
		public final ColumnReader<String> fkSchema = new ColumnReader<>(addColumn("FKTABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Foreign key table name being exported */
		public final ColumnReader<String> fkTableName = new ColumnReader<>(addColumn("FKTABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Foreign key column name being exported */
		public final ColumnReader<String> fkColumnName = new ColumnReader<>(addColumn("FKCOLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key). */
		public final ColumnReader<Short> sequence = new ColumnReader<>(addColumn("KEY_SEQ", short.class), ResultSet::getShort);
		
		/**
		 * What happens to foreign key when parent key is updated:
		 * - {@link DatabaseMetaData#importedKeyNoAction} : do not allow update of parent key if it has been imported
		 * - {@link DatabaseMetaData#importedKeyCascade} : change imported key to agree with parent key update
		 * - {@link DatabaseMetaData#importedKeySetNull} : change imported key to NULL if its parent key has been updated
		 * - {@link DatabaseMetaData#importedKeySetDefault} : change imported key to default values if its parent key has been updated
		 * - {@link DatabaseMetaData#importedKeyRestrict} : same as {@link DatabaseMetaData#importedKeyNoAction} (for ODBC 2.x compatibility)
		 */
		public final ColumnReader<Short> updateRule = new ColumnReader<>(addColumn("UPDATE_RULE", short.class), ResultSet::getShort);
		
		/**
		 * What happens to the foreign key when parent key is deleted:
		 * - {@link DatabaseMetaData#importedKeyNoAction} : do not allow delete of parent key if it has been imported
		 * - {@link DatabaseMetaData#importedKeyCascade} : delete rows that import a deleted key
		 * - {@link DatabaseMetaData#importedKeySetNull} : change imported key to NULL if its primary key has been deleted
		 * - {@link DatabaseMetaData#importedKeySetDefault} : change imported key to default if its parent key has been deleted
		 * - {@link DatabaseMetaData#importedKeyRestrict} : same as {@link DatabaseMetaData#importedKeyNoAction} (for ODBC 2.x compatibility)
		 */
		public final ColumnReader<Short> deleteRule = new ColumnReader<>(addColumn("DELETE_RULE", short.class), ResultSet::getShort);
		
		
		/** Foreign key name (may be null) */
		public final ColumnReader<String> fkName = new ColumnReader<>(addColumn("FK_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Parent key name (may be null) */
		public final ColumnReader<String> pkName = new ColumnReader<>(addColumn("PK_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * Can the evaluation of foreign key constraints be deferred until commit:
		 * - {@link DatabaseMetaData#importedKeyInitiallyDeferred} : see SQL92 for definition
		 * - {@link DatabaseMetaData#importedKeyInitiallyImmediate} : see SQL92 for definition
		 * - {@link DatabaseMetaData#importedKeyNotDeferrable} : see SQL92 for definition
		 */
		public final ColumnReader<Short> deferrability = new ColumnReader<>(addColumn("DEFERRABILITY", short.class), ResultSet::getShort);
		
		public ExportedKeysMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getExportedKeys() or DatabaseMetaData.getImportedKeys() ResultSet
			super("ForeignKeyMetaData");
		}
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}
	 * @author Guillaume Mary
	 */
	public static class PrimaryKeysMetaDataPseudoTable extends Table<PrimaryKeysMetaDataPseudoTable> {
		
		public static final PrimaryKeysMetaDataPseudoTable INSTANCE = new PrimaryKeysMetaDataPseudoTable();
		
		/** table catalog (may be null) */
		public final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table schema (may be null) */
		public final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table name */
		public final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** column name */
		public final ColumnReader<String> columnName = new ColumnReader<>(addColumn("COLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/**
		 * sequence number within primary key( a value of 1 represents the first column of the primary key,
		 * a value of 2 would represent the second column within the primary key).
		 */
		public final ColumnReader<String> columnIndex = new ColumnReader<>(addColumn("KEY_SEQ", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** primary key name (may be null) */
		public final ColumnReader<String> name = new ColumnReader<>(addColumn("PK_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		
		public PrimaryKeysMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getPrimaryKeys() ResultSet
			super("PrimaryKeysMetaData");
		}
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 * @author Guillaume Mary
	 */
	public static class ColumnMetaDataPseudoTable extends Table<ColumnMetaDataPseudoTable> {
		
		public static final ColumnMetaDataPseudoTable INSTANCE = new ColumnMetaDataPseudoTable();
		
		public final ColumnReader<String> catalog = new ColumnReader<>(addColumn("TABLE_CAT", String.class), DefaultResultSetReaders.STRING_READER);
		
		public final ColumnReader<String> schema = new ColumnReader<>(addColumn("TABLE_SCHEM", String.class), DefaultResultSetReaders.STRING_READER);
		
		public final ColumnReader<String> tableName = new ColumnReader<>(addColumn("TABLE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		public final ColumnReader<String> columnName = new ColumnReader<>(addColumn("COLUMN_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** SQL type from {@link java.sql.Types} */
		public final ColumnReader<Integer> type = new ColumnReader<>(addColumn("DATA_TYPE", int.class), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		
		/** Data source dependent type name, for a User-Defined-Type the type name is fully qualified */
		public final ColumnReader<String> typeName = new ColumnReader<>(addColumn("TYPE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
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
		public final ColumnReader<Integer> size = new ColumnReader<>(addColumn("COLUMN_SIZE", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** not used */
		public final ColumnReader<Object> bufferLength = new ColumnReader<>(addColumn("BUFFER_LENGTH", Object.class), ResultSet::getObject);
		
		/** the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable. */
		public final ColumnReader<Integer> decimalDigits = new ColumnReader<>(addColumn("DECIMAL_DIGITS", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** Radix (typically either 10 or 2) */
		public final ColumnReader<Integer> radix = new ColumnReader<>(addColumn("NUM_PREC_RADIX", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/**
		 * Is null allowed, possible values
		 * {@link DatabaseMetaData#columnNoNulls}
		 * {@link DatabaseMetaData#columnNullable}
		 * {@link DatabaseMetaData#columnNullableUnknown}
		 * Transformed as Boolean :
		 * - true if {@link DatabaseMetaData#columnNullable}
		 * - false otherwise
		 */
		public final ColumnReader<Boolean> nullable = new ColumnReader<>(addColumn("NULLABLE", boolean.class), (rs, col) -> rs.getInt(col) == DatabaseMetaData.columnNullable);
		
		/** comment describing column (may be null) */
		public final ColumnReader<String> remarks = new ColumnReader<>(addColumn("REMARKS", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** Default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null) */
		public final ColumnReader<String> defaultValue = new ColumnReader<>(addColumn("COLUMN_DEF", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** unused */
		public final ColumnReader<Integer> sqlDataType = new ColumnReader<>(addColumn("SQL_DATA_TYPE", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** unused */
		public final ColumnReader<Integer> sqlDatetimeSub = new ColumnReader<>(addColumn("SQL_DATETIME_SUB", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/** for char types the maximum number of bytes in the column */
		public final ColumnReader<Integer> charOctetLength = new ColumnReader<>(addColumn("CHAR_OCTET_LENGTH", Integer.class), DefaultResultSetReaders.INTEGER_READER);
		
		/**
		 * index of column in table (starting at 1)
		 */
		public final ColumnReader<Integer> ordinalPosition = new ColumnReader<>(addColumn("ORDINAL_POSITION", Integer.class), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		
		/**
		 * ISO rules are used to determine the nullability for a column, possible original values
		 * - YES : if the column can include NULLs
		 * - NO : if the column cannot include NULLs
		 * - empty string : if the nullability for the column is unknown
		 * Transformed as Boolean :
		 * - true if YES
		 * - false otherwise
		 */
		public final ColumnReader<Boolean> isNullable = new ColumnReader<>(addColumn("IS_NULLABLE", boolean.class), (rs, col) -> "yes".equalsIgnoreCase(rs.getString(col)));
		
		/** catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF) */
		public final ColumnReader<String> scopeCatalog = new ColumnReader<>(addColumn("SCOPE_CATALOG", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF) */
		public final ColumnReader<String> scopeSchema = new ColumnReader<>(addColumn("SCOPE_SCHEMA", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF) */
		public final ColumnReader<String> scopeTable = new ColumnReader<>(addColumn("SCOPE_TABLE", String.class), DefaultResultSetReaders.STRING_READER);
		
		/** source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF) */
		public final ColumnReader<Short> sourceDataType = new ColumnReader<>(addColumn("SOURCE_DATA_TYPE", Short.class), ResultSet::getShort);
		
		/**
		 * Indicates whether this column is auto incremented, possible original values :
		 * - YES : if the column is auto incremented
		 * - NO : if the column is not auto incremented
		 * - empty string : if it cannot be determined whether the column is auto incremented
		 * Transformed as Boolean :
		 * - true if YES
		 * - false otherwise
		 */
		public final ColumnReader<Boolean> isAutoIncrement = new ColumnReader<>(addColumn("IS_AUTOINCREMENT", boolean.class), (rs, col) -> "yes".equalsIgnoreCase(rs.getString(col)));
		
		/**
		 * Indicates whether this is a generated column, possible original values :
		 * - YES : if this a generated column
		 * - NO : if this not a generated column
		 * - empty string : if it cannot be determined whether this is a generated column
		 * Transformed as Boolean :
		 * - true if YES
		 * - false otherwise
		 */
		public final ColumnReader<Boolean> isGeneratedColumn = new ColumnReader<>(addColumn("IS_GENERATEDCOLUMN", boolean.class), (rs, col) -> "yes".equalsIgnoreCase(rs.getString(col)));
		
		public ColumnMetaDataPseudoTable() {
			// This table has no real name, it's made to map DatabaseMetaData.getColumns() ResultSet
			super("ColumnMetaData");
		}
	}
	
	public static class ColumnReader<T> {
		
		public final Column<?, T> column;
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
