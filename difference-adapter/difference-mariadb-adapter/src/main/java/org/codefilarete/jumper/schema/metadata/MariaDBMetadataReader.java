package org.codefilarete.jumper.schema.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;

public class MariaDBMetadataReader extends DefaultMetadataReader implements SequenceMetadataReader {
	
	public MariaDBMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}

	@Override
	public SortedSet<ColumnMetadata> giveColumns(String catalog, String schema, String tablePattern) {
		try (Connection connection = metaData.getConnection();
			 ResultSet tableResultSet = connection.prepareStatement("select" +
							 " c.TABLE_SCHEMA as " + ColumnMetaDataPseudoTable.INSTANCE.schema.column.getName() + "," +
							 " c.TABLE_CATALOG as " + ColumnMetaDataPseudoTable.INSTANCE.catalog.column.getName() + "," +
							 " c.TABLE_NAME as " + ColumnMetaDataPseudoTable.INSTANCE.tableName.column.getName() + "," +
							 " c.COLUMN_NAME," +
							 " c.COLUMN_TYPE," +
							 " c.TABLE_NAME," +
							 " c.DATA_TYPE," +
							 " coalesce(c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_SCALE) as size," +
							 " c.COLUMN_TYPE," +
							 " c.IS_NULLABLE," +
							 " c.IS_GENERATED," +
							 " c.NUMERIC_PRECISION as decimalDigits," +
							 " c.ORDINAL_POSITION" +
							 " from information_schema.COLUMNS c" +""
//							 " where c.TABLE_CATALOG " + (catalog == null || "".equals(catalog) ? "like '%'" : ("= '" + schema +"'")) +
//					 (schema == null ? "" : " and c.TABLE_SCHEMA like '%" + schema + "%'") +
//							 " and c.TABLE_NAME like '%" + tablePattern + "%'"
					 )
					 .executeQuery()) {
//		try (ResultSet tableResultSet = metaData.getColumns(catalog, schema, tablePattern, "%")) {
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
