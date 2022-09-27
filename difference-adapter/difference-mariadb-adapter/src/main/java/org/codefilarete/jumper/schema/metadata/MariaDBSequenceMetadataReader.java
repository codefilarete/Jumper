package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;

public class MariaDBSequenceMetadataReader extends DefaultMetadataReader implements SequenceMetadataReader {
	
	public MariaDBSequenceMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
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
			
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
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
