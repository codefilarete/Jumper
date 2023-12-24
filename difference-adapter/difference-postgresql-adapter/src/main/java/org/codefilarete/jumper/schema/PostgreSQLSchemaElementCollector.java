package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.MetadataReader;
import org.codefilarete.jumper.schema.metadata.PostgreSQLSequenceMetadataReader;
import org.codefilarete.jumper.schema.metadata.SequenceMetadata;
import org.codefilarete.jumper.schema.metadata.SequenceMetadataReader;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

public class PostgreSQLSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public PostgreSQLSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new PostgreSQLSequenceMetadataReader(databaseMetaData));
	}
	
	public PostgreSQLSchemaElementCollector(MetadataReader metadataReader) {
		super(metadataReader);
	}
	
	@Override
	public PostgreSQLSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	@Override
	public PostgreSQLSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	@Override
	public PostgreSQLSchemaElementCollector withTableNamePattern(String tableNamePattern) {
		super.withTableNamePattern(tableNamePattern);
		return this;
	}
	
	@Override
	protected void completeSchema(Schema result) {
		if (metadataReader instanceof SequenceMetadataReader) {
			Set<SequenceMetadata> sequenceMetadata = ((SequenceMetadataReader) metadataReader).giveSequences(catalog, schema);
			sequenceMetadata.forEach(row -> ((PostgreSQLSchema) result).addSequence(row.getName()));
		}
	}
	
	@Override
	public PostgreSQLSchema collect() {
		return (PostgreSQLSchema) super.collect();
	}
	
	@Override
	protected PostgreSQLSchema createSchema(StringAppender schemaName) {
		return new PostgreSQLSchema(Strings.preventEmpty(schemaName.toString(), null));
	}
	
	public static class PostgreSQLSchema extends Schema {
		
		private final Set<PostgreSQLSchema.Sequence> sequences = new HashSet<>();
		
		public PostgreSQLSchema(String name) {
			super(name);
		}
		
		public Set<PostgreSQLSchema.Sequence> getSequences() {
			return sequences;
		}
		
		void addSequence(String name) {
			PostgreSQLSchema.Sequence result = new PostgreSQLSchema.Sequence(name);
			this.sequences.add(result);
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
