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
 * Class aimed at extracting table columns metadata for MariaDB, inspired from
 *
 * @author Guillaume Mary
 */
public class ExportedKeysMetadataReader {
	
	private final DatabaseMetaData metaData;
	
	public ExportedKeysMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}
	
	public ResultSet giveMetaData(Operator schema, Operator tableNamePattern)
			throws SQLException {
		String indexSelectSQL = "SELECT NULL PKTABLE_CAT, KCU.REFERENCED_TABLE_SCHEMA PKTABLE_SCHEM, KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,"
								+ " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, NULL FKTABLE_CAT, KCU.TABLE_SCHEMA FKTABLE_SCHEM, "
								+ " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
								+ " CASE update_rule "
								+ "   WHEN 'RESTRICT' THEN 1"
								+ "   WHEN 'NO ACTION' THEN 3"
								+ "   WHEN 'CASCADE' THEN 0"
								+ "   WHEN 'SET NULL' THEN 2"
								+ "   WHEN 'SET DEFAULT' THEN 4"
								+ " END UPDATE_RULE,"
								+ " CASE DELETE_RULE"
								+ "  WHEN 'RESTRICT' THEN 1"
								+ "  WHEN 'NO ACTION' THEN 3"
								+ "  WHEN 'CASCADE' THEN 0"
								+ "  WHEN 'SET NULL' THEN 2"
								+ "  WHEN 'SET DEFAULT' THEN 4"
								+ " END DELETE_RULE,"
								+ " RC.CONSTRAINT_NAME FK_NAME,"
								+ " RC.UNIQUE_CONSTRAINT_NAME PK_NAME,"
								+ DatabaseMetaData.importedKeyNotDeferrable
								+ " DEFERRABILITY"
								+ " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
								+ " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
								+ " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
								+ " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME";
		
		PreparedCriteria[] criteria = Stream.of(
						asSQLCriteria("KCU.REFERENCED_TABLE_SCHEMA", schema),
						asSQLCriteria("KCU.REFERENCED_TABLE_NAME", tableNamePattern))
				.filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		indexSelectSQL += " WHERE " + Stream.of(criteria)
				.map(PreparedCriteria::getCriteriaSegment)
				.collect(Collectors.joining(" AND "));
		indexSelectSQL += " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
		
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
