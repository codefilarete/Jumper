package org.codefilarete.jumper.schema.metadata;

import javax.annotation.Nullable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.jumper.schema.metadata.PreparedCriteria.Operator;

import static org.codefilarete.jumper.schema.metadata.PreparedCriteria.asSQLCriteria;

/**
 * Class aimed at extracting table indexes metadata for MariaDB
 *
 * @author Guillaume Mary
 */
public class IndexMetadataReader extends AbstractMetadataReader {
	
	public static final String INDEX_SELECT_SQL_BASE = "SELECT NULL TABLE_CAT, "
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
	
	public IndexMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	ResultSet giveMetaData(@Nullable Operator<String> schema,
						   @Nullable Operator<String> tableNamePattern,
						   @Nullable Operator<Boolean> unique)
			throws SQLException {
		
		StringBuilder indexSelectSQL = new StringBuilder(INDEX_SELECT_SQL_BASE);
		
		PreparedCriteria[] criteria = Stream.of(
						asSQLCriteria("TABLE_SCHEMA", schema),
						asSQLCriteria("TABLE_NAME", tableNamePattern),
						asSQLCriteria("NON_UNIQUE", unique))
				.filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		indexSelectSQL.append(" WHERE ").append(Stream.of(criteria)
				.map(PreparedCriteria::getCriteriaSegment)
				.collect(Collectors.joining(" AND ")));
		indexSelectSQL.append(" ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION");
		
		return executeQuery(indexSelectSQL, criteria);
	}
}
