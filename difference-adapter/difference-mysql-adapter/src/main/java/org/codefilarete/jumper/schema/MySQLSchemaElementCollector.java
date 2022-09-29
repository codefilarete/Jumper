package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;

import org.codefilarete.jumper.schema.metadata.DefaultMetadataReader;
import org.codefilarete.jumper.schema.metadata.IndexMetadata;
import org.codefilarete.jumper.schema.metadata.MetadataReader;

public class MySQLSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public MySQLSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new DefaultMetadataReader(databaseMetaData));
	}
	
	public MySQLSchemaElementCollector(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	public MySQLSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	public MySQLSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	public MySQLSchemaElementCollector withTableNamePattern(String tableNamePattern) {
		super.withTableNamePattern(tableNamePattern);
		return this;
	}
	
	@Override
	protected boolean shouldAddIndex(Schema result, IndexMetadata row) {
		// we don't consider adding index related to primary key to schema since they highly linked to it
		if (row.getName().equals("PRIMARY")) {
			return false;
		} else {
			return super.shouldAddIndex(result, row);
		}
	}
}
