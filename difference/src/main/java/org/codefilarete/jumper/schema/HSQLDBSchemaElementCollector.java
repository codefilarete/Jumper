package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.DefaultMetadataReader;
import org.codefilarete.jumper.schema.metadata.MetadataReader;
import org.codefilarete.jumper.schema.metadata.SequenceMetadata;
import org.codefilarete.jumper.schema.metadata.SequenceMetadataReader;

public class HSQLDBSchemaElementCollector extends SchemaElementCollector {
	
	public HSQLDBSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new DefaultMetadataReader(databaseMetaData));
	}
	
	public HSQLDBSchemaElementCollector(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	public HSQLDBSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	public HSQLDBSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	public HSQLDBSchemaElementCollector withTableNamePattern(String tableNamePattern) {
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
	public HSQLDBSchema collect() {
		return (HSQLDBSchema) super.collect();
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
