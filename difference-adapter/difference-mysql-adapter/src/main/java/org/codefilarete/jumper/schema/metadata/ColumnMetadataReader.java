package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.codefilarete.tool.trace.ModifiableInt;

/**
 * Class aimed at extracting table columns metadata for MySQL
 *
 * @author Guillaume Mary
 */
public class ColumnMetadataReader {
	
//	private final Configuration configuration;
	private final DatabaseMetaData metaData;
	
	public ColumnMetadataReader(DatabaseMetaData metaData) {
		this.metaData = metaData;
//		this.configuration = buildConfiguration(this.metaData);
	}
	
	ResultSet buildGiveColumnsStatement(Operator schema, Operator tableNamePattern, Operator columnNamePattern) throws SQLException {
		String columnSelectSQL = "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,"
				+ dataTypeClause("COLUMN_TYPE") + " DATA_TYPE,"
				+ dataTypeClause() + " TYPE_NAME, "
				+ " CASE DATA_TYPE"
				+ "  WHEN 'time' THEN "
				+ "IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))"
				+ "  WHEN 'date' THEN 10"
				+ "  WHEN 'datetime' THEN "
				+ "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
				+ "  WHEN 'timestamp' THEN "
				+ "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
				+ " WHEN 'year' THEN 5"
//				+ (configuration.isYearIsDateType() ? "" : " WHEN 'year' THEN 5")
				+ "  ELSE "
				+ "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"
				+ Integer.MAX_VALUE
				+ "), NUMERIC_PRECISION) "
				+ " END"
				+ " COLUMN_SIZE, 65535 BUFFER_LENGTH, "
				+ " CONVERT (CASE DATA_TYPE"
				+ " WHEN 'year' THEN "
				+ "NUMERIC_SCALE"
				+ " WHEN 'tinyint' THEN "
				+ "NUMERIC_SCALE"
				+ " ELSE NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS,"
				+ " 10 NUM_PREC_RADIX, IF(IS_NULLABLE = 'yes',1,0) NULLABLE,COLUMN_COMMENT REMARKS,"
				+ " COLUMN_DEFAULT COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB,  "
				+ " LEAST(CHARACTER_OCTET_LENGTH,"
				+ Integer.MAX_VALUE
				+ ") CHAR_OCTET_LENGTH,"
				+ " ORDINAL_POSITION, IS_NULLABLE, NULL SCOPE_CATALOG, NULL SCOPE_SCHEMA, NULL SCOPE_TABLE, NULL SOURCE_DATA_TYPE,"
				+ " IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT, "
				+ " IF(EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED') ,'YES','NO') IS_GENERATEDCOLUMN "
				+ " FROM INFORMATION_SCHEMA.COLUMNS";
		PreparedCriteria[] criteria = Stream.of(asSQLCriteria("TABLE_SCHEMA", schema),
				asSQLCriteria("TABLE_NAME", tableNamePattern),
				asSQLCriteria("COLUMN_NAME", columnNamePattern)).filter(Objects::nonNull).toArray(PreparedCriteria[]::new);
		columnSelectSQL += " WHERE " + Stream.of(criteria)
				.map(preparedCriteria -> preparedCriteria.criteriaSegment)
				.collect(Collectors.joining(" AND "));
		columnSelectSQL += " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
		
		PreparedStatement preparedStatement = metaData.getConnection().prepareStatement(columnSelectSQL);
		ModifiableInt preparedParameterIndex = new ModifiableInt(0);
		Stream.of(criteria)
				.flatMap(preparedCriteria -> preparedCriteria.values.stream())
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
	
//	Configuration buildConfiguration(DatabaseMetaData metaData) {
//		final Configuration configuration;
//		org.mariadb.jdbc.Configuration conf;
//		boolean noBackslashEscapes;
//		try {
//			org.mariadb.jdbc.Connection connection = (org.mariadb.jdbc.Connection) metaData.getConnection();
//			conf = connection.getContext().getConf();
//			noBackslashEscapes = (connection.getContext().getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;
//		} catch (SQLException e) {
//			throw new RuntimeException(e);
//		}
//		;
//		return new Configuration(noBackslashEscapes, conf.tinyInt1isBit(), conf.yearIsDateType());
//	}
	
	PreparedCriteria asSQLCriteria(String columnName, Operator<?> operator) {
		if (operator == null) {
			return null;
		}
		PreparedCriteria operatorCriterion = new PreparedCriteria();
		if (operator instanceof Like) {
			operatorCriterion.criteriaSegment = columnName + " like ?";
			operatorCriterion.values = Arrays.asList((String) operator.getValue());
		} else if (operator instanceof Equal) {
			operatorCriterion.criteriaSegment = columnName + " = ?";
			operatorCriterion.values = Arrays.asList((String) operator.getValue());
		} else if (operator instanceof In) {
			operatorCriterion.criteriaSegment = columnName + " in (" + IntStream.of(((Set<String>) operator.getValue()).size()).mapToObj(i -> "?").collect(Collectors.joining(", ")) + ")";
			operatorCriterion.values = (Set<String>) operator.getValue();
		}
		return operatorCriterion;
	}
	
	public static class PreparedCriteria {
		
		private String criteriaSegment;
		
		private Collection<String> values;
		
		public String getCriteriaSegment() {
			return criteriaSegment;
		}
		
		public Collection<String> getValues() {
			return values;
		}
	}
	
	public static abstract class Operator<V> {
		
		protected V value;
		
		public Operator(V value) {
			this.value = value;
		}
		
		public V getValue() {
			return value;
		}
	}
	
	public static class Like<V extends String> extends Operator<V> {
		
		public Like(V value) {
			super(value);
		}
		
	}
	
	public static class Equal<V extends String> extends Operator<V> {
		
		public Equal(V value) {
			super(value);
		}
	}
	
	public static class In<V extends String> extends Operator<Set<V>> {
		
		public In(Set<V> value) {
			super(value);
		}
	}
	
	private String dataTypeClause(String fullTypeColumnName) {
		return " CASE data_type"
				+ " WHEN 'bit' THEN "
				+ Types.BIT
				+ " WHEN 'tinyblob' THEN "
				+ Types.VARBINARY
				+ " WHEN 'mediumblob' THEN "
				+ Types.LONGVARBINARY
				+ " WHEN 'longblob' THEN "
				+ Types.LONGVARBINARY
				+ " WHEN 'blob' THEN "
				+ Types.LONGVARBINARY
				+ " WHEN 'tinytext' THEN "
				+ Types.VARCHAR
				+ " WHEN 'mediumtext' THEN "
				+ Types.LONGVARCHAR
				+ " WHEN 'longtext' THEN "
				+ Types.LONGVARCHAR
				+ " WHEN 'text' THEN "
				+ Types.LONGVARCHAR
				+ " WHEN 'date' THEN "
				+ Types.DATE
				+ " WHEN 'datetime' THEN "
				+ Types.TIMESTAMP
				+ " WHEN 'decimal' THEN "
				+ Types.DECIMAL
				+ " WHEN 'double' THEN "
				+ Types.DOUBLE
				+ " WHEN 'enum' THEN "
				+ Types.VARCHAR
				+ " WHEN 'float' THEN "
				+ Types.REAL
				+ " WHEN 'int' THEN IF( "
				+ fullTypeColumnName
				+ " like '%unsigned%', "
				+ Types.INTEGER
				+ ","
				+ Types.INTEGER
				+ ")"
				+ " WHEN 'bigint' THEN "
				+ Types.BIGINT
				+ " WHEN 'mediumint' THEN "
				+ Types.INTEGER
				+ " WHEN 'null' THEN "
				+ Types.NULL
				+ " WHEN 'set' THEN "
				+ Types.VARCHAR
				+ " WHEN 'smallint' THEN IF( "
				+ fullTypeColumnName
				+ " like '%unsigned%', "
				+ Types.SMALLINT
				+ ","
				+ Types.SMALLINT
				+ ")"
				+ " WHEN 'varchar' THEN "
				+ Types.VARCHAR
				+ " WHEN 'varbinary' THEN "
				+ Types.VARBINARY
				+ " WHEN 'char' THEN "
				+ Types.CHAR
				+ " WHEN 'binary' THEN "
				+ Types.BINARY
				+ " WHEN 'time' THEN "
				+ Types.TIME
				+ " WHEN 'timestamp' THEN "
				+ Types.TIMESTAMP
				+ " WHEN 'tinyint' THEN "
//				+ (configuration.isTinyInt1isBit()
//				? "IF("
//				+ fullTypeColumnName
//				+ " like 'tinyint(1)%',"
//				+ Types.BIT
//				+ ","
//				+ Types.TINYINT
//				+ ") "
//				: Types.TINYINT)
				+ Types.TINYINT
				+ " WHEN 'year' THEN "
				+ Types.DATE
//				+ (configuration.isYearIsDateType() ? Types.DATE : Types.SMALLINT)
				+ " ELSE "
				+ Types.OTHER
				+ " END ";
	}
	
	private String dataTypeClause() {
		String upperCaseWithoutSize =
				" UCASE(IF( COLUMN_TYPE LIKE '%(%)%', CONCAT(SUBSTRING( COLUMN_TYPE,1, LOCATE('(',"
						+ "COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE ,1+locate(')', COLUMN_TYPE))), "
						+ "COLUMN_TYPE))";
		
//		if (configuration.isTinyInt1isBit()) {
//			upperCaseWithoutSize = " IF(COLUMN_TYPE like 'tinyint(1)%', 'BIT', " + upperCaseWithoutSize + ")";
//		}
//		if (!configuration.isYearIsDateType()) {
//			return " IF(COLUMN_TYPE IN ('year(2)', 'year(4)'), 'SMALLINT', " + upperCaseWithoutSize + ")";
//		}
		return upperCaseWithoutSize;
	}
	
	public static class Configuration {
		
		private final boolean noBackslashEscapes;
		private final boolean tinyInt1isBit;
		private final boolean yearIsDateType;
		
		public Configuration(boolean noBackslashEscapes, boolean tinyInt1isBit, boolean yearIsDateType) {
			this.noBackslashEscapes = noBackslashEscapes;
			this.tinyInt1isBit = tinyInt1isBit;
			this.yearIsDateType = yearIsDateType;
		}
		
		public boolean isNoBackslashEscapes() {
			return noBackslashEscapes;
		}
		
		public boolean isTinyInt1isBit() {
			return tinyInt1isBit;
		}
		
		public boolean isYearIsDateType() {
			return yearIsDateType;
		}
	}
}
