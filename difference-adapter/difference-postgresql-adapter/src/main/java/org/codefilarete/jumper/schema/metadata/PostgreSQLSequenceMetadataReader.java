package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.tool.Strings;

public class PostgreSQLSequenceMetadataReader extends DefaultMetadataReader implements SequenceMetadataReader {
	
	public PostgreSQLSequenceMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	@Override
	public Set<SequenceMetadata> giveSequences(String catalog, String schema) {
		String namespace = null;
		if (!Strings.isEmpty(catalog)) {
			namespace = catalog;
		}
		if (!Strings.isEmpty(schema)) {
			if (!Strings.isEmpty(namespace)) {
				namespace += "." + schema;
			} else {
				namespace = schema;
			}
		}
		
		String namespaceCriteria = null;
		if (!Strings.isEmpty(namespace)) {
			String operator;
			if (namespace.contains("%")) {
				operator = "like";
			} else {
				operator = "=";
			}
			namespaceCriteria = operator + " '" + namespace + "'";
		}
		String sequenceSql = "SELECT"
				+ " s.schemaname AS \"SCHEMA_NAME\", "
				+ " s.sequencename AS \"SEQUENCE_NAME\", "
				+ " s.min_value AS \"MIN_VALUE\","
				+ " s.max_value AS \"MAX_VALUE\","
				+ " s.increment_by AS \"INCREMENT_BY\","
				+ " s.cycle AS \"WILL_CYCLE\","
				+ " s.start_value AS \"START_VALUE\""
				+ " FROM pg_sequences s";
		
//		if (namespaceCriteria != null) {
//			sequenceSql += " WHERE s.schemaname " + namespaceCriteria;
//		}
		
		Set<SequenceMetadata> result = new HashSet<>();
		try (PreparedStatement selectSequenceStatement = metaData.getConnection().prepareStatement(sequenceSql);
			 ResultSet tableResultSet = selectSequenceStatement.executeQuery()) {
			ResultSetIterator<SequenceMetadata> resultSetIterator = new ResultSetIterator<SequenceMetadata>(tableResultSet) {
				@Override
				public SequenceMetadata convert(ResultSet resultSet) {
					return new SequenceMetadata(
							catalog,
							SequenceMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							SequenceMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet)
					);
				}
			};
			
			result.addAll(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return result;
	}
	
	/**
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 * @author Guillaume Mary
	 */
	static class SequenceMetaDataPseudoTable extends Table<SequenceMetaDataPseudoTable> {
		
		static final SequenceMetaDataPseudoTable INSTANCE = new SequenceMetaDataPseudoTable();
		
		private final ColumnReader<String> schema = new ColumnReader<>(addColumn("SCHEMA_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<String> name = new ColumnReader<>(addColumn("SEQUENCE_NAME", String.class), DefaultResultSetReaders.STRING_READER);
		
		private final ColumnReader<Long> startValue = new ColumnReader<>(addColumn("START_VALUE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> minValue = new ColumnReader<>(addColumn("MIN_VALUE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> maxValue = new ColumnReader<>(addColumn("MAX_VALUE", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Long> incrementBy = new ColumnReader<>(addColumn("INCREMENT_BY", long.class), DefaultResultSetReaders.LONG_PRIMITIVE_READER);
		
		private final ColumnReader<Boolean> willCycle = new ColumnReader<>(addColumn("WILL_CYCLE", boolean.class), DefaultResultSetReaders.BOOLEAN_PRIMITIVE_READER);
		
		public SequenceMetaDataPseudoTable() {
			// This table has no real name, it's made to map query on information_schema to retrieve views
			super("SequenceMetaData");
		}
	}
}
