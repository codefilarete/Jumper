package org.codefilarete.jumper.schema.metadata;

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
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.metadata.PreparedCriteria.Equal;
import org.codefilarete.jumper.schema.metadata.PreparedCriteria.Like;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.tool.Nullable;

public class MySQLMetadataReader extends DefaultMetadataReader implements SequenceMetadataReader {
	
	private final TableMetadataReader tableMetadataReader;
	
	private final ColumnMetadataReader columnMetadataReader;
	
	private final IndexMetadataReader indexMetadataReader;
	
	private final ExportedKeysMetadataReader exportedKeysMetadataReader;
	
	public MySQLMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
		this.tableMetadataReader = new TableMetadataReader(metaData);
		this.columnMetadataReader = new ColumnMetadataReader(metaData);
		this.indexMetadataReader = new IndexMetadataReader(metaData);
		this.exportedKeysMetadataReader = new ExportedKeysMetadataReader(metaData);
	}
	
	@Override
	public Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern) {
		try (ResultSet tableResultSet = tableMetadataReader.giveMetaData(
				Nullable.nullable(schema).map(Like::new).get(),
				Nullable.nullable(tableNamePattern).map(Like::new).get())) {
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
	public SortedSet<ColumnMetadata> giveColumns(String catalog, String schema, String tablePattern) {
		try (ResultSet tableResultSet = columnMetadataReader.buildGiveColumnsStatement(Nullable.nullable(schema).map(Like::new).get(), new Like<>(tablePattern), null)) {
			ResultSetIterator<ColumnMetadata> resultSetIterator = new ResultSetIterator<ColumnMetadata>(tableResultSet) {
				@Override
				public ColumnMetadata convert(ResultSet resultSet) {
					ColumnMetadata result = new ColumnMetadata(
							ColumnMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							ColumnMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
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
			SortedSet<ColumnMetadata> result = new TreeSet<>(
					Comparator.comparing(ColumnMetadata::getTableName)
							.thenComparing(ColumnMetadata::getPosition));
			result.addAll(resultSetIterator.convert());
			return result;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Set<IndexMetadata> giveIndexes(String catalog, String schema, String tablePattern, Boolean unique) {
		try (ResultSet tableResultSet = indexMetadataReader.giveMetaData(
				Nullable.nullable(schema).map(Like::new).get(),
				Nullable.nullable(tablePattern).map(Like::new).get(),
				Nullable.nullable(unique).map(Equal::new).get())) {
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
	public Set<ForeignKeyMetadata> giveExportedKeys(String catalog, String schema, String tablePattern) {
		try (ResultSet tableResultSet = exportedKeysMetadataReader.giveMetaData(Nullable.nullable(schema).map(Like::new).get(), new Like<>(tablePattern))) {
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
			schemaCriteria = "TABLE_SCHEMA " + schemaCriteria;
		}
		
		String sequenceNameSql = "select"
				+ " table_name AS SEQUENCE_NAME,"
				+ " table_schema as SEQUENCE_SCHEMA from information_schema.TABLES";
		
		String criteria = "TABLE_TYPE = 'SEQUENCE'";
		if (schemaCriteria != null) {
			criteria += " AND " + schemaCriteria;
		}
		
		sequenceNameSql += " WHERE " + criteria;
		sequenceNameSql += " order by table_name";
		
		class Sequence {
			private final String schema;
			private final String name;
			
			Sequence(String schema, String name) {
				this.schema = schema;
				this.name = name;
			}
		}
		
		Set<Sequence> sequenceNames;
		try (PreparedStatement selectSequenceStatement = metaData.getConnection().prepareStatement(sequenceNameSql);
			 ResultSet tableResultSet = selectSequenceStatement.executeQuery()) {
			ResultSetIterator<Sequence> resultSetIterator = new ResultSetIterator<Sequence>(tableResultSet) {
				@Override
				public Sequence convert(ResultSet resultSet) {
					return new Sequence(
							SequenceNameMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							SequenceNameMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet));
				}
			};
			sequenceNames = new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
		Set<SequenceMetadata> result = new HashSet<>();
		if (!sequenceNames.isEmpty()) {
			String sequencesDetailsSql = sequenceNames.stream().map(sequenceName -> "SELECT" +
							" '" + sequenceName.schema + "' AS SEQUENCE_SCHEMA," +
							" '" + sequenceName.name + "' AS SEQUENCE_NAME," +
							" START_VALUE AS START_VALUE," +
							" MINIMUM_VALUE AS MIN_VALUE," +
							" MAXIMUM_VALUE AS MAX_VALUE," +
							" INCREMENT AS INCREMENT_BY," +
							" CYCLE_OPTION AS WILL_CYCLE" +
							" FROM " + sequenceName.schema + "." + sequenceName.name)
					.collect(Collectors.joining(" union "));
			
			try (PreparedStatement selectSequenceStatement = metaData.getConnection().prepareStatement(sequencesDetailsSql);
				 ResultSet tableResultSet = selectSequenceStatement.executeQuery()) {
				ResultSetIterator<SequenceMetadata> resultSetIterator = new ResultSetIterator<SequenceMetadata>(tableResultSet) {
					@Override
					public SequenceMetadata convert(ResultSet resultSet) {
						return new SequenceMetadata(
								catalog,
								SequenceNameMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
								SequenceNameMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet)
						);
					}
				};
				
				result.addAll(resultSetIterator.convert());
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return result;
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 *
	 * @author Guillaume Mary
	 */
	static class SequenceMetaDataPseudoTable extends Table<SequenceMetaDataPseudoTable> {
		
		static final SequenceMetaDataPseudoTable INSTANCE = new SequenceMetaDataPseudoTable();
		
		private final ColumnReader<Long> startValue = new ColumnReader<>(addColumn("START_VALUE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> minValue = new ColumnReader<>(addColumn("MIN_VALUE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> maxValue = new ColumnReader<>(addColumn("MAX_VALUE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> incrementBy = new ColumnReader<>(addColumn("INCREMENT_BY", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> willCycle = new ColumnReader<>(addColumn("WILL_CYCLE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		public SequenceMetaDataPseudoTable() {
			// This table has no real name, it's made to map query on information_schema to retrieve views
			super("SequenceMetaData");
		}
	}
	
	static class SequenceNameMetaDataPseudoTable extends Table<SequenceNameMetaDataPseudoTable> {
		
		static final SequenceNameMetaDataPseudoTable INSTANCE = new SequenceNameMetaDataPseudoTable();
		
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("SEQUENCE_SCHEMA", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> name = new ColumnReader<>(addColumn("SEQUENCE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		public SequenceNameMetaDataPseudoTable() {
			// This table has no real name, it's made to map query on information_schema to retrieve sequences
			super("SequenceNameMetaData");
		}
	}
}
