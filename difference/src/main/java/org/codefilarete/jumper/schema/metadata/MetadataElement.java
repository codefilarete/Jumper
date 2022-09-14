package org.codefilarete.jumper.schema.metadata;

/**
 * A marking interface for all elements of a catalog / schema structure, such as Tables, Columns, Indexes, etc
 *
 * @author Guillaume Mary
 */
public interface MetadataElement {
	
	/**
	 * Classes implementing this interface are considered in a catalog and / or schema
	 *
	 * @author Guillaume Mary
	 */
	interface SchemaNamespaceElement {
		
		String getCatalog();
		
		String getSchema();
		
	}
	
	/**
	 * Classes implementing this interface are considered in a table
	 *
	 * @author Guillaume Mary
	 */
	interface TableNamespaceElement extends SchemaNamespaceElement {
		
		String getTableName();
		
	}
	
	class SchemaNamespaceElementSupport implements SchemaNamespaceElement {
		
		private final String catalog;
		private final String schema;
		
		public SchemaNamespaceElementSupport(String catalog, String schema) {
			this.catalog = catalog;
			this.schema = schema;
		}
		
		public String getCatalog() {
			return catalog;
		}
		
		public String getSchema() {
			return schema;
		}
	}
	
	class TableNamespaceElementSupport extends SchemaNamespaceElementSupport implements TableNamespaceElement {
		
		private final String tableName;
		
		public TableNamespaceElementSupport(String catalog, String schema, String tableName) {
			super(catalog, schema);
			this.tableName = tableName;
		}
		
		@Override
		public String getTableName() {
			return tableName;
		}
	}
	
}
