package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.jumper.schema.metadata.PreparedCriteria.Operator;

import static org.codefilarete.jumper.schema.metadata.PreparedCriteria.asSQLCriteria;

public class PrimaryKeyMetadataReader extends AbstractMetadataReader {
	
	public static final String PRIMARY_KEY_SELECT_SQL_BASE = "SELECT"
			+ " NULL TABLE_CAT, A.TABLE_SCHEMA TABLE_SCHEM,"
			+ " A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, B.INDEX_NAME PK_NAME "
			+ " FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B"
			// MySQL 8 now use 'PRI' in place of 'pri'
			+ " WHERE A.COLUMN_KEY in ('PRI','pri') AND B.INDEX_NAME='PRIMARY'"
			+ " AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME";
	
	public PrimaryKeyMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	public ResultSet giveMetaData(Operator schema, Operator tableNamePattern) throws SQLException {
		StringBuilder indexSelectSQL = new StringBuilder(PRIMARY_KEY_SELECT_SQL_BASE);
		
		PreparedCriteria[] criteria = Stream.of(
						asSQLCriteria("A.TABLE_SCHEMA", schema),
						asSQLCriteria("A.TABLE_NAME", tableNamePattern))
				.filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		indexSelectSQL.append(" AND ").append(Stream.of(criteria)
				.map(PreparedCriteria::getCriteriaSegment)
				.collect(Collectors.joining(" AND ")));
		indexSelectSQL.append(" ORDER BY A.COLUMN_NAME");
		
		return executeQuery(indexSelectSQL, criteria);
	}
}
