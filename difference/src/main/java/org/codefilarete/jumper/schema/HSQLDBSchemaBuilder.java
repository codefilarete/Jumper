package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.DefaultMetadataReader;
import org.codefilarete.jumper.schema.metadata.MetadataReader;
import org.codefilarete.jumper.schema.metadata.SequenceMetadata;
import org.codefilarete.jumper.schema.metadata.SequenceMetadataReader;

public class HSQLDBSchemaBuilder extends SchemaBuilder {
	
	public HSQLDBSchemaBuilder(DatabaseMetaData databaseMetaData) {
		this(new DefaultMetadataReader(databaseMetaData));
	}
	
	public HSQLDBSchemaBuilder(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	public HSQLDBSchemaBuilder withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	public HSQLDBSchemaBuilder withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	public HSQLDBSchemaBuilder withTableNamePattern(String tableNamePattern) {
		super.withTableNamePattern(tableNamePattern);
		return this;
	}
	
	@Override
	protected void completeSchema(Schema result) {
		if (metadataReader instanceof SequenceMetadataReader) {
			Set<SequenceMetadata> sequenceMetadata = ((SequenceMetadataReader) metadataReader).giveSequences(catalog, schema);
			sequenceMetadata.forEach(row -> ((HSQLDBSchema) result).addSequence(row.getName()));
		}
	}
	
	// TODO: instantiate right schema type
	
	@Override
	public HSQLDBSchema build() {
		return (HSQLDBSchema) super.build();
	}
	
	public static class HSQLDBSchema extends Schema {
		
		private final Set<Schema.Sequence> sequences = new HashSet<>();
		
		public HSQLDBSchema(String name) {
			super(name);
		}
		
		public Set<Schema.Sequence> getSequences() {
			return sequences;
		}
		
		Schema.Sequence addSequence(String name) {
			Schema.Sequence result = new Schema.Sequence(name);
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
