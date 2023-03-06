package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.IndexMetadata;
import org.codefilarete.jumper.schema.metadata.MariaDBMetadataReader;
import org.codefilarete.jumper.schema.metadata.MetadataReader;
import org.codefilarete.jumper.schema.metadata.SequenceMetadata;
import org.codefilarete.jumper.schema.metadata.SequenceMetadataReader;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

public class MariaDBSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public MariaDBSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new MariaDBMetadataReader(databaseMetaData));
	}
	
	public MariaDBSchemaElementCollector(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	@Override
	public MariaDBSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	@Override
	public MariaDBSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	@Override
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
	protected boolean shouldAddIndex(Schema result, IndexMetadata metadata) {
		// we don't consider adding index related to primary key to schema since they highly linked to it
		if (metadata.getName().equals("PRIMARY")) {
			return false;
		} else {
			return super.shouldAddIndex(result, metadata);
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
