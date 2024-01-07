package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.HSQLDBSequenceMetadataReader;
import org.codefilarete.jumper.schema.metadata.SchemaMetadataReader;
import org.codefilarete.jumper.schema.metadata.SequenceMetadata;
import org.codefilarete.jumper.schema.metadata.SequenceMetadataReader;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

public class HSQLDBSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public HSQLDBSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new HSQLDBSequenceMetadataReader(databaseMetaData));
	}
	
	public HSQLDBSchemaElementCollector(SchemaMetadataReader metadataReader) {
		super(metadataReader);
	}
	
	@Override
	public HSQLDBSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	@Override
	public HSQLDBSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	@Override
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
	
	@Override
	public HSQLDBSchema collect() {
		return (HSQLDBSchema) super.collect();
	}
	
	@Override
	protected HSQLDBSchema createSchema(StringAppender schemaName) {
		return new HSQLDBSchema(Strings.preventEmpty(schemaName.toString(), null));
	}
	
	public static class HSQLDBSchema extends Schema {
		
		private final Set<HSQLDBSchema.Sequence> sequences = new HashSet<>();
		
		public HSQLDBSchema(String name) {
			super(name);
		}
		
		public Set<HSQLDBSchema.Sequence> getSequences() {
			return sequences;
		}
		
		HSQLDBSchema.Sequence addSequence(String name) {
			HSQLDBSchema.Sequence result = new HSQLDBSchema.Sequence(name);
			this.sequences.add(result);
			return result;
		}
		
		public class Sequence {
			
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
