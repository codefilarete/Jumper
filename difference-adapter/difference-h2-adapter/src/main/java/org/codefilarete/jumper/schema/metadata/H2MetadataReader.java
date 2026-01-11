package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.SortedSet;

import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.NUMERIC;
import static java.sql.JDBCType.REAL;

/**
 * Metadata reader for H2 database.
 * 
 * @author Guillaume Mary
 */
public class H2MetadataReader extends DefaultMetadataReader {
	
	private static final Set<JDBCType> TYPES_WITH_PRECISION = Collections.unmodifiableSet(EnumSet.of(DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE));
	
	public H2MetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	@Override
	public SortedSet<ColumnMetadata> giveColumns(String catalog, String schema, String tablePattern) {
		SortedSet<ColumnMetadata> superResult = super.giveColumns(catalog, schema, tablePattern);
		superResult.forEach(columnMetadata -> {
			// H2 set precision to 0 for non-decimal numeric types, we clear it because it doesn't make sense
			if (!TYPES_WITH_PRECISION.contains(columnMetadata.getSqlType())) {
				columnMetadata.setPrecision(null);
			}
		});
		return superResult;
	}
}
