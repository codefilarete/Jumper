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
public class TableMetadataReader extends AbstractMetadataReader {
	
	private static final String INDEX_SELECT_SQL_BASE = "SELECT NULL TABLE_CAT, TABLE_SCHEMA  TABLE_SCHEM,  TABLE_NAME,"
			+ " IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE) as TABLE_TYPE,"
			+ " TABLE_COMMENT REMARKS, NULL TYPE_CAT, NULL TYPE_SCHEM, NULL TYPE_NAME, NULL SELF_REFERENCING_COL_NAME, "
			+ " NULL REF_GENERATION"
			+ " FROM INFORMATION_SCHEMA.TABLES";
	
	public TableMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	public ResultSet giveMetaData(Operator schema, Operator tableNamePattern) throws SQLException {
		StringBuilder indexSelectSQL = new StringBuilder(INDEX_SELECT_SQL_BASE);
		
		
		PreparedCriteria[] criteria = Stream.of(
						asSQLCriteria("TABLE_SCHEMA", schema),
						asSQLCriteria("TABLE_NAME", tableNamePattern))
				.filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		indexSelectSQL.append(" WHERE ").append(Stream.of(criteria)
				.map(PreparedCriteria::getCriteriaSegment)
				.collect(Collectors.joining(" AND ")));
		indexSelectSQL.append(" ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");
		
		return executeQuery(indexSelectSQL, criteria);
	}
}

