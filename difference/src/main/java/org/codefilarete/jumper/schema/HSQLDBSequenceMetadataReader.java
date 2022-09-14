package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.DefaultMetadataReader.ColumnReader;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;

public class HSQLDBSequenceMetadataReader implements SequenceMetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public HSQLDBSequenceMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
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
	 * Pseudo table representing columns given by {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 * @author Guillaume Mary
	 */
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
}
