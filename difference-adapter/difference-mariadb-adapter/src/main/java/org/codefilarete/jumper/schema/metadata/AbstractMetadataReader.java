package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.codefilarete.tool.trace.MutableInt;

public abstract class AbstractMetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public AbstractMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}
	
	protected ResultSet executeQuery(StringBuilder columnsSelectSQL, PreparedCriteria[] criteria) throws SQLException {
		PreparedStatement preparedStatement = metaData.getConnection().prepareStatement(columnsSelectSQL.toString());
		MutableInt preparedParameterIndex = new MutableInt(0);
		Stream.of(criteria)
				.flatMap(preparedCriteria -> preparedCriteria.getValues().stream())
				.map(String.class::cast)
				.forEach(value -> {
					try {
						preparedStatement.setString(preparedParameterIndex.increment(), value);
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				});
		return preparedStatement.executeQuery();
	}
}