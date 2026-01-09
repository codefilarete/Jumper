package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.types.Type;

public class HSQLDBMetadataReader extends DefaultMetadataReader implements SequenceMetadataReader {
	
	private static final String SELSTAR = "SELECT * FROM INFORMATION_SCHEMA.";
	
	private boolean useSchemaDefault;
	
	public HSQLDBMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
		try {
			useSchemaDefault = ((JDBCConnection) metaData.getConnection()).getConnProperties().isPropertyTrue(HsqlDatabaseProperties.url_default_schema,
					false);
		} catch (SQLException e) {
			useSchemaDefault = false;
		}
		
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
	
	@Override
	public Set<ProcedureMetadata> giveProcedures(String catalog, String schema, String procedurePatternName) {
		try (ResultSet tableResultSet = getProcedures(catalog, schema, procedurePatternName)) {
			ResultSetIterator<ProcedureMetadata> resultSetIterator = new ResultSetIterator<ProcedureMetadata>(tableResultSet) {
				@Override
				public ProcedureMetadata convert(ResultSet resultSet) {
					ProcedureMetadata result = new ProcedureMetadata(
							ProcedureMetaDataPseudoTable.INSTANCE.catalog.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
							ProcedureMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet)
					);
					ProcedureMetaDataPseudoTable.INSTANCE.remarks.apply(resultSet, result::setRemarks);
					ProcedureMetaDataPseudoTable.INSTANCE.procedureType.apply(resultSet, procedureType -> result.setType(ProcedureMetadata.ProcedureType.valueOf(procedureType)));
					ProcedureMetaDataPseudoTable.INSTANCE.specificName.apply(resultSet, result::setSpecificName);
					return result;
				}
			};
			return new HashSet<>(resultSetIterator.convert());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	private ResultSet getProcedures(String catalog, String schema, String procedurePatternName) throws SQLException {
		StringBuilder sb = new StringBuilder();
		
		sb.append("select procedure_cat, procedure_schem, procedure_name, ")
				.append(
						"col_4, col_5, col_6, remarks, procedure_type, specific_name ")
				.append("from information_schema.system_procedures ")
				// fixing issue here: HSQLDB returns only functions, not all procedure, we remove the filter
				// and replace it with "1=1" to ensure other criteria concatenation
//				.append("where procedure_type = 1 ");
				.append("where 1 = 1 ");
		
		if (wantsIsNull(procedurePatternName)) {
			sb.append("and 1=0");
			
			return execute(sb.toString());
		}
		
		catalog = translateCatalog(catalog);
		schema = translateSchema(schema);
		
		sb.append(and("PROCEDURE_CAT", "=", catalog))
				.append(and("PROCEDURE_SCHEM", "LIKE", schema))
				.append(and("PROCEDURE_NAME", "LIKE", procedurePatternName))
				.append(
						" ORDER BY PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME");
		
		return execute(sb.toString());
	}
	
	ResultSet execute(String sql) {
		try {
			Statement st = metaData.getConnection().createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			return st.executeQuery(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private ResultSet executeSelect(
			String table,
			String where)
			throws SQLException {
		
		String select = SELSTAR + table;
		
		if (where != null) {
			select += " WHERE " + where;
		}
		
		return execute(select);
	}
	
	private static boolean wantsIsNull(String s) {
		return (s != null && s.isEmpty());
	}
	
	String getDatabaseDefaultSchema() throws SQLException {
		
		final ResultSet rs = executeSelect("SYSTEM_SCHEMAS", "IS_DEFAULT=TRUE");
		String          value = rs.next()
				? rs.getString(1)
				: null;
		
		rs.close();
		
		return value;
	}
	
	/**
	 * For compatibility, when the connection property "default_schema=true"
	 * is present, any DatabaseMetaData call with an empty string as the
	 * schema parameter will use the default schema (normally "PUBLIC").
	 */
	private String translateSchema(String schemaName) throws SQLException {
		
		if (useSchemaDefault && schemaName != null && schemaName.isEmpty()) {
			final String result = getDatabaseDefaultSchema();
			
			if (result != null) {
				schemaName = result;
			}
		}
		
		return schemaName;
	}
	
	/**
	 * Returns the name of the catalog of the default schema.
	 */
	String getDatabaseDefaultCatalog() throws SQLException {
		
		final ResultSet rs = executeSelect("SYSTEM_SCHEMAS", "IS_DEFAULT=TRUE");
		String          value = rs.next()
				? rs.getString(2)
				: null;
		
		rs.close();
		
		return value;
	}
	
	private String translateCatalog(String catalogName) throws SQLException {
		
		if (useSchemaDefault && catalogName != null && catalogName.isEmpty()) {
			String result = getDatabaseDefaultCatalog();
			
			if (result != null) {
				catalogName = result;
			}
		}
		
		return catalogName;
	}
	
	private static String and(String id, String op, Object val) {
		
		// The JDBC standard for pattern arguments seems to be:
		//
		// - pass null to mean ignore (do not include in query),
		// - pass "" to mean filter on <column-ident> IS NULL,
		// - pass "%" to filter on <column-ident> IS NOT NULL.
		// - pass sequence with "%" and "_" for wildcard matches
		// - when searching on values reported directly from DatabaseMetaData
		//   results, typically an exact match is desired.  In this case, it
		//   is the client's responsibility to escape any reported "%" and "_"
		//   characters using whatever DatabaseMetaData returns from
		//   getSearchEscapeString(). In our case, this is the standard escape
		//   character: '\'. Typically, '%' will rarely be encountered, but
		//   certainly '_' is to be expected on a regular basis.
		// - checkme:  what about the (silly) case where an identifier
		//   has been declared such as:  'create table "xxx\_yyy"(...)'?
		//   Must the client still escape the Java string like this:
		//   "xxx\\\\_yyy"?
		//   Yes: because otherwise the driver is expected to
		//   construct something like:
		//   select ... where ... like 'xxx\_yyy' escape '\'
		//   which will try to match 'xxx_yyy', not 'xxx\_yyy'
		//   Testing indicates that indeed, higher quality popular JDBC
		//   database browsers do the escapes "properly."
		if (val == null) {
			return "";
		}
		
		StringBuilder sb    = new StringBuilder();
		boolean       isStr = (val instanceof String);
		
		if (isStr && ((String) val).isEmpty()) {
			return sb.append(" AND ").append(id).append(" IS NULL").toString();
		}
		
		String v = isStr
				? Type.SQL_VARCHAR.convertToSQLString(val)
				: String.valueOf(val);
		
		sb.append(" AND ").append(id).append(' ');
		
		// add the escape to like if required
		if (isStr && "LIKE".equalsIgnoreCase(op)) {
			if (v.indexOf('_') < 0 && v.indexOf('%') < 0) {
				
				// then we can optimize.
				sb.append("=").append(' ').append(v);
			} else {
				sb.append("LIKE").append(' ').append(v);
				
				if (v.contains("\\_") || v.contains("\\%")) {
					
					// then client has requested at least one escape.
					sb.append(" ESCAPE '\\'");
				}
			}
		} else {
			sb.append(op).append(' ').append(v);
		}
		
		return sb.toString();
	}
}
