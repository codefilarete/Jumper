package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.jumper.schema.metadata.PreparedCriteria.Operator;
import org.codefilarete.tool.trace.ModifiableInt;

import static org.codefilarete.jumper.schema.metadata.PreparedCriteria.asSQLCriteria;

/**
 * Class aimed at extracting table indexes metadata for MariaDB
 *
 * @author Guillaume Mary
 */
public class IndexMetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public IndexMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}
	
	ResultSet giveMetaData(Operator schema, Operator tableNamePattern) throws SQLException {
		String indexSelectSQL = "SELECT NULL TABLE_CAT, "
								+ "TABLE_SCHEMA TABLE_SCHEM, "
								+ "TABLE_NAME, "
								+ "NON_UNIQUE, "
								+ "TABLE_SCHEMA INDEX_QUALIFIER, "
								+ "INDEX_NAME, "
								+ DatabaseMetaData.tableIndexOther
								+ " TYPE, "
								+ "SEQ_IN_INDEX ORDINAL_POSITION, "
								+ "COLUMN_NAME, "
								+ "COLLATION ASC_OR_DESC, "
								+ "CARDINALITY, "
								+ "NULL PAGES, "
								+ "NULL FILTER_CONDITION"
								+ " FROM INFORMATION_SCHEMA.STATISTICS";
		
		PreparedCriteria[] criteria = Stream.of(
				asSQLCriteria("TABLE_SCHEMA", schema),
				asSQLCriteria("TABLE_NAME", tableNamePattern))
				.filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		indexSelectSQL += " WHERE " + Stream.of(criteria)
				.map(PreparedCriteria::getCriteriaSegment)
				.collect(Collectors.joining(" AND "));
		indexSelectSQL += " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";
		
		PreparedStatement preparedStatement = metaData.getConnection().prepareStatement(indexSelectSQL);
		ModifiableInt preparedParameterIndex = new ModifiableInt(0);
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
