package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;

import org.codefilarete.jumper.schema.metadata.IndexMetadata;
import org.codefilarete.jumper.schema.metadata.MetadataReader;
import org.codefilarete.jumper.schema.metadata.MySQLMetadataReader;

public class MySQLSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public MySQLSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new MySQLMetadataReader(databaseMetaData));
	}
	
	public MySQLSchemaElementCollector(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	@Override
	public MySQLSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	@Override
	public MySQLSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	@Override
	public MySQLSchemaElementCollector withTableNamePattern(String tableNamePattern) {
		super.withTableNamePattern(tableNamePattern);
		return this;
	}
	
	@Override
	protected boolean shouldAddIndex(Schema result, IndexMetadata indexMetadata) {
		// we don't consider adding index related to primary key to schema since they highly linked to it
		if (indexMetadata.getName().equals("PRIMARY")) {
			return false;
		} else {
			return super.shouldAddIndex(result, indexMetadata);
		}
	}
}
