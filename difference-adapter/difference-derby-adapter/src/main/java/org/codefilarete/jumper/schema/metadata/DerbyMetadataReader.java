package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.util.Set;

import org.codefilarete.tool.Strings;

public class DerbyMetadataReader extends DefaultMetadataReader {
	
	public DerbyMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * Overridden to align behavior with other vendors by setting empty Remarks to null
	 */
	@Override
	protected Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern, String[] tableTypes) {
		Set<TableMetadata> tables = super.giveTables(catalog, schema, tableNamePattern, tableTypes);
		// set empty Remarks to null from the result (to align the behavior with other vendors)
		tables.forEach(tableMetadata -> {
			if (Strings.isEmpty(tableMetadata.getRemarks())) {
				tableMetadata.setRemarks(null);
			}
		});
		return tables;
	}
}
