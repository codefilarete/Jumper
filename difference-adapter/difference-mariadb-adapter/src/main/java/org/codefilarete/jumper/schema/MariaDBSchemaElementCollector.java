package org.codefilarete.jumper.schema;

import org.codefilarete.jumper.schema.metadata.*;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

public class MariaDBSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public MariaDBSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new MariaDBMetadataReader(databaseMetaData));
	}
	
	public MariaDBSchemaElementCollector(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	public MariaDBSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	public MariaDBSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	public MariaDBSchemaElementCollector withTableNamePattern(String tableNamePattern) {
		super.withTableNamePattern(tableNamePattern);
		return this;
	}
	
	@Override
	protected void completeSchema(Schema result) {
		if (metadataReader instanceof SequenceMetadataReader) {
			Set<SequenceMetadata> sequenceMetadata = ((SequenceMetadataReader) metadataReader).giveSequences(catalog, schema);
			sequenceMetadata.forEach(row -> ((MariaDBSchema) result).addSequence(row.getName()));
		}
	}
	
	@Override
	public MariaDBSchema collect() {
		return (MariaDBSchema) super.collect();
	}
	
	@Override
	protected MariaDBSchema createSchema(StringAppender schemaName) {
		return new MariaDBSchema(Strings.preventEmpty(schemaName.toString(), null));
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
	
	public static class MariaDBSchema extends Schema {
		
		private final Set<MariaDBSchema.Sequence> sequences = new HashSet<>();
		
		public MariaDBSchema(String name) {
			super(name);
		}
		
		public Set<MariaDBSchema.Sequence> getSequences() {
			return sequences;
		}
		
		MariaDBSchema.Sequence addSequence(String name) {
			MariaDBSchema.Sequence result = new MariaDBSchema.Sequence(name);
			this.sequences.add(result);
			return result;
		}
		
		protected class Sequence {
			
			private final String name;
			
			protected Sequence(String name) {
				this.name = name;
			}
			
			public String getName() {
				return name;
			}
		}
	}
}
