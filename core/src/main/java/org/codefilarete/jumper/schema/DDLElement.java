package org.codefilarete.jumper.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A marking interface for all elements of a catalog / schema structure, such as Tables, Columns, Indexes, etc
 *
 * @author Guillaume Mary
 */
public interface DDLElement {
	
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
	
	class ProcedureMetadata extends SchemaNamespaceElementSupport implements DDLElement {
		
		private final String name;
		private final String remarks;
		private final short type;
		private final String specificName;
		
		public ProcedureMetadata(String catalog, String schema, String name, String remarks, short type, String specificName) {
			super(catalog, schema);
			this.name = name;
			this.remarks = remarks;
			this.type = type;
			this.specificName = specificName;
		}
		
		public String getName() {
			return name;
		}
		
		public String getRemarks() {
			return remarks;
		}
		
		public short getType() {
			return type;
		}
		
		public String getSpecificName() {
			return specificName;
		}
	}
	
	class IndexMetadata extends TableNamespaceElementSupport implements DDLElement {
		
		private String name;
		private short type;
		private boolean unique;
		private String indexQualifier;
		private short ordinalPosition;
		private List<Duo<String, Boolean>> columns = new ArrayList<>();
		private long cardinality;
		private long pages;
		private String filterCondition;
		
		public IndexMetadata(String catalog, String schema, String tableName) {
			super(catalog, schema, tableName);
		}
		
		public String getName() {
			return name;
		}
		
		void setName(String name) {
			this.name = name;
		}
		
		public short getType() {
			return type;
		}
		
		void setType(short type) {
			this.type = type;
		}
		
		public boolean isUnique() {
			return unique;
		}
		
		void setUnique(boolean unique) {
			this.unique = unique;
		}
		
		public String getIndexQualifier() {
			return indexQualifier;
		}
		
		void setIndexQualifier(String indexQualifier) {
			this.indexQualifier = indexQualifier;
		}
		
		public short getOrdinalPosition() {
			return ordinalPosition;
		}
		
		void setOrdinalPosition(short ordinalPosition) {
			this.ordinalPosition = ordinalPosition;
		}
		
		public List<Duo<String, Boolean>> getColumns() {
			return columns;
		}
		
		void addColumn(String columnName, Boolean ascOrDesc) {
			this.columns.add(new Duo<>(columnName, ascOrDesc));
		}
		
		public long getCardinality() {
			return cardinality;
		}
		
		void setCardinality(long cardinality) {
			this.cardinality = cardinality;
		}
		
		public long getPages() {
			return pages;
		}
		
		void setPages(long pages) {
			this.pages = pages;
		}
		
		public String getFilterCondition() {
			return filterCondition;
		}
		
		void setFilterCondition(String filterCondition) {
			this.filterCondition = filterCondition;
		}
	}
	
	class ViewMetadata extends SchemaNamespaceElementSupport implements DDLElement {
		
		private String name;
		
		public ViewMetadata(String catalog, String schema) {
			super(catalog, schema);
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
	}
	
	class TableMetadata extends SchemaNamespaceElementSupport implements DDLElement {
		
		private String name;
		private String type;
		private String remarks;
		private String typeCatalog;
		private String typeSchema;
		private String typeName;
		private String selfReferencingColName;
		private String refGeneration;
		
		public TableMetadata(String catalog, String schema) {
			super(catalog, schema);
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getType() {
			return type;
		}
		
		public void setType(String type) {
			this.type = type;
		}
		
		public String getRemarks() {
			return remarks;
		}
		
		public void setRemarks(String remarks) {
			this.remarks = remarks;
		}
		
		public String getTypeCatalog() {
			return typeCatalog;
		}
		
		public void setTypeCatalog(String typeCatalog) {
			this.typeCatalog = typeCatalog;
		}
		
		public String getTypeSchema() {
			return typeSchema;
		}
		
		public void setTypeSchema(String typeSchema) {
			this.typeSchema = typeSchema;
		}
		
		public String getTypeName() {
			return typeName;
		}
		
		public void setTypeName(String typeName) {
			this.typeName = typeName;
		}
		
		public String getSelfReferencingColName() {
			return selfReferencingColName;
		}
		
		public void setSelfReferencingColName(String selfReferencingColName) {
			this.selfReferencingColName = selfReferencingColName;
		}
		
		public String getRefGeneration() {
			return refGeneration;
		}
		
		public void setRefGeneration(String refGeneration) {
			this.refGeneration = refGeneration;
		}
	}
	
	class SequenceMetadata extends SchemaNamespaceElementSupport implements DDLElement {
		
		private final String name;
		
		public SequenceMetadata(String catalog, String schema, String name) {
			super(catalog, schema);
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	}
	
	class ColumnMetadata extends TableNamespaceElementSupport implements DDLElement {
		
		private String name;
		private int sqlType;
		private String vendorType;
		private Integer size;
		private Integer precision;
		private boolean nullable;
		private boolean autoIncrement;
		private int position;
		
		public ColumnMetadata(String catalog, String schema, String tableName) {
			super(catalog, schema, tableName);
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public int getSqlType() {
			return sqlType;
		}
		
		public void setSqlType(int sqlType) {
			this.sqlType = sqlType;
		}
		
		public String getVendorType() {
			return vendorType;
		}
		
		public void setVendorType(String vendorType) {
			this.vendorType = vendorType;
		}
		
		public Integer getSize() {
			return size;
		}
		
		public void setSize(Integer size) {
			this.size = size;
		}
		
		public Integer getPrecision() {
			return precision;
		}
		
		public void setPrecision(Integer precision) {
			this.precision = precision;
		}
		
		public boolean isNullable() {
			return nullable;
		}
		
		public void setNullable(boolean nullable) {
			this.nullable = nullable;
		}
		
		public boolean isAutoIncrement() {
			return autoIncrement;
		}
		
		public void setAutoIncrement(boolean autoIncrement) {
			this.autoIncrement = autoIncrement;
		}
		
		public int getPosition() {
			return position;
		}
		
		public void setPosition(int position) {
			this.position = position;
		}
		
		@Override
		public String toString() {
			return "Column{" +
					"name='" + name + '\'' +
					", sqlType=" + sqlType +
					", vendorType='" + vendorType + '\'' +
					", size=" + size +
					", nullable=" + nullable +
					", autoIncrement=" + autoIncrement +
					'}';
		}
	}
	
	class PrimaryKeyMetadata extends TableNamespaceElementSupport implements DDLElement {
		
		private final String name;
		private final KeepOrderSet<String> columns = new KeepOrderSet<>();
		
		public PrimaryKeyMetadata(String catalog, String schema, String tableName,
								  String name) {
			super(catalog, schema, tableName);
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		void addColumn(String columnName) {
			this.columns.add(columnName);
		}
		
		public KeepOrderSet<String> getColumns() {
			return this.columns;
		}
		
		@Override
		public String toString() {
			return "PrimaryKey{" +
					"name='" + name + '\'' +
					", sourceTable=" + getTableName() +
					", columns=" + columns.getSurrogate() +
					'}';
		}
	}
	
	class ForeignKeyMetadata implements DDLElement {
		
		private final String name;
		private final TableNamespaceElement sourceTable;
		private final TableNamespaceElement targetTable;
		private final Set<Duo<String, String>> columns = new KeepOrderSet<>();
		
		public ForeignKeyMetadata(String name,
								  String sourceCatalog, String sourceSchema, String sourceTableName,
								  String targetCatalog, String targetSchema, String targetTableName) {
			this.name = name;
			this.sourceTable = new TableNamespaceElementSupport(sourceCatalog, sourceSchema, sourceTableName);
			this.targetTable = new TableNamespaceElementSupport(targetCatalog, targetSchema, targetTableName);
		}
		
		public String getName() {
			return name;
		}
		
		public TableNamespaceElement getSourceTable() {
			return sourceTable;
		}
		
		public TableNamespaceElement getTargetTable() {
			return targetTable;
		}
		
		/**
		 * Gives all pairs of column names associated by this foreign key : left side is source, right side is referenced
		 * @return pairs of columns, from source to target, in order defined by this foreign key
		 */
		public Set<Duo<String, String>> getColumns() {
			return columns;
		}
		
		void addColumn(String sourceColumnName, String targetColumnName) {
			this.columns.add(new Duo<>(sourceColumnName, targetColumnName));
		}
		
		@Override
		public String toString() {
			return "ForeignKey{" +
					"name='" + name + '\'' +
					", sourceTable=" + sourceTable.getTableName() +
					", targetTable=" + targetTable.getTableName() +
					", columns=" + columns.stream().map(duo -> duo.getLeft() + " = " + duo.getRight()).collect(Collectors.toList()) +
					'}';
		}
	}
}
