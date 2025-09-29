package org.codefilarete.jumper.schema.metadata;


import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.jumper.schema.metadata.PreparedCriteria.Operator;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.jumper.schema.metadata.PreparedCriteria.asSQLCriteria;


/**
 * Class aimed at extracting table metadata for MariaDB
 *
 * @author Guillaume Mary
 */
public class TableMetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public TableMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}
	
	public ResultSet giveMetaData(Operator schema, Operator tableNamePattern) throws SQLException {
		String indexSelectSQL = "SELECT NULL TABLE_CAT, TABLE_SCHEMA  TABLE_SCHEM,  TABLE_NAME,"
				+ " IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE) as TABLE_TYPE,"
				+ " TABLE_COMMENT REMARKS, NULL TYPE_CAT, NULL TYPE_SCHEM, NULL TYPE_NAME, NULL SELF_REFERENCING_COL_NAME, "
				+ " NULL REF_GENERATION"
				+ " FROM INFORMATION_SCHEMA.TABLES";
		
		
		PreparedCriteria[] criteria = Stream.of(
						asSQLCriteria("TABLE_SCHEMA", schema),
						asSQLCriteria("TABLE_NAME", tableNamePattern))
				.filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		indexSelectSQL += " WHERE " + Stream.of(criteria)
				.map(PreparedCriteria::getCriteriaSegment)
				.collect(Collectors.joining(" AND "));
		indexSelectSQL += " ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME";
		
		
		PreparedStatement preparedStatement = metaData.getConnection().prepareStatement(indexSelectSQL);
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

