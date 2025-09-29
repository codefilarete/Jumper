package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.IndexMetadata;
import org.codefilarete.jumper.schema.metadata.SchemaMetadataReader;
import org.codefilarete.jumper.schema.metadata.MySQLMetadataReader;
import org.codefilarete.jumper.schema.metadata.SequenceMetadata;
import org.codefilarete.jumper.schema.metadata.SequenceMetadataReader;
import org.codefilarete.tool.Strings;

public class MySQLSchemaElementCollector extends DefaultSchemaElementCollector {
	
	public MySQLSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new MySQLMetadataReader(databaseMetaData));
	}
	
	public MySQLSchemaElementCollector(SchemaMetadataReader metadataReader) {
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
	protected void completeSchema(Schema result) {
		if (metadataReader instanceof SequenceMetadataReader) {
			Set<SequenceMetadata> sequenceMetadata = ((SequenceMetadataReader) metadataReader).giveSequences(catalog, schema);
			sequenceMetadata.forEach(row -> ((MySQLSchema) result).addSequence(row.getName()));
		}
	}
	
	@Override
	public MySQLSchema collect() {
		return (MySQLSchema) super.collect();
	}
	
	@Override
	protected MySQLSchema createSchema(String schemaName) {
		return new MySQLSchema(Strings.preventEmpty(schemaName, null));
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
	
	public static class MySQLSchema extends Schema {
		
		private final Set<Sequence> sequences = new HashSet<>();
		
		public MySQLSchema(String name) {
			super(name);
		}
		
		public Set<MySQLSchema.Sequence> getSequences() {
			return sequences;
		}
		
		void addSequence(String name) {
			MySQLSchema.Sequence result = new MySQLSchema.Sequence(name);
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
