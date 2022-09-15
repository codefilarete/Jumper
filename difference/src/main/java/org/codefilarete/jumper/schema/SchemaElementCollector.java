package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.AscOrDesc;
import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.View;
import org.codefilarete.jumper.schema.metadata.ColumnMetadata;
import org.codefilarete.jumper.schema.metadata.DefaultMetadataReader;
import org.codefilarete.jumper.schema.metadata.ForeignKeyMetadata;
import org.codefilarete.jumper.schema.metadata.IndexMetadata;
import org.codefilarete.jumper.schema.metadata.MetadataReader;
import org.codefilarete.jumper.schema.metadata.PrimaryKeyMetadata;
import org.codefilarete.jumper.schema.metadata.TableMetadata;
import org.codefilarete.jumper.schema.metadata.ViewMetadata;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.PairIterator;

/**
 * Collects database schema elements based on a {@link MetadataReader} to read DDL elements and build a structured
 * version of them through a {@link Schema}.
 *
 * @see #collect()
 * @author Guillaume Mary
 */
public class SchemaElementCollector {
	
	protected final MetadataReader metadataReader;
	protected String catalog;
	protected String schema;
	protected String tableNamePattern;
	
	/**
	 * Constructor to create schema elements from given {@link DatabaseMetaData}.
	 * Will use a {@link DefaultMetadataReader} to collect elements and create a {@link Schema} on {@link #collect()}
	 * call.
	 *
	 * @param databaseMetaData
	 */
	public SchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new DefaultMetadataReader(databaseMetaData));
	}
	
	/**
	 * Generic constructor. Will create {@link Schema} elements from given {@link MetadataReader} on {@link #collect()}
	 * call.
	 *
	 * @param metadataReader
	 */
	public SchemaElementCollector(MetadataReader metadataReader) {
		this.metadataReader = metadataReader;
	}
	
	public SchemaElementCollector withCatalog(String catalog) {
		this.catalog = catalog;
		return this;
	}
	
	public SchemaElementCollector withSchema(String schema) {
		this.schema = schema;
		return this;
	}
	
	public SchemaElementCollector withTableNamePattern(String tableNamePattern) {
		this.tableNamePattern = tableNamePattern;
		return this;
	}
	
	public Schema collect() {
		StringAppender schemaName = new StringAppender();
		schemaName.catIf(!Strings.isEmpty(catalog), catalog);
		schemaName.catIf(schemaName.length() != 0 && !Strings.isEmpty(schema), "." + schema);
		
		Schema result = new Schema(Strings.preventEmpty(schemaName.toString(), null));
		
		// Collecting tables
		Set<TableMetadata> tableMetadata = metadataReader.giveTables(catalog, schema, tableNamePattern);
		Map<String, Table> tablePerName = new HashMap<>();
		tableMetadata.forEach(row -> {
			Table table = result.addTable(row.getName());
			table.setComment(row.getRemarks());
			tablePerName.put(table.name, table);
		});
		
		// Collecting columns and sewing them with tables
		Map<Duo<String, String>, Column> columnCache = new HashMap<>();
		tablePerName.values().forEach(table -> {
			Set<ColumnMetadata> columnMetadata = metadataReader.giveColumns(catalog, schema, table.name);
			columnMetadata.forEach(row -> {
				Column column = table.addColumn(row.getName(),
						row.getVendorType(), row.getSize(), row.getPrecision(),
						row.isNullable(), row.isAutoIncrement());
				columnCache.put(new Duo<>(table.name, row.getName()), column);
			});
			PrimaryKeyMetadata primaryKeyMetadata = metadataReader.givePrimaryKey(catalog, schema, table.name);
			if (primaryKeyMetadata != null) {
				List<Column> primaryKeyColumns = new ArrayList<>();
				primaryKeyMetadata.getColumns().forEach(columnName -> {
					primaryKeyColumns.add(columnCache.get(new Duo<>(table.name, columnName)));
				});
				table.setPrimaryKey(primaryKeyColumns);
			}
		});
		
		// Collecting Foreign Keys
		// Some databases support table name pattern in getExportedKeys(..), some not. Since giving a pattern is not
		// possible according to getExportedKeys(..) specification, we have to iterate over found table names to build
		// foreign keys (again, since we just iterated over them to build Table instances) : we can't add this building
		// to previous iteration because foreign key building needs source and target Columns which are not all created
		// as we iterate to build Tables.
		// For database vendors supporting pattern, this could be done without going over each table (and maybe enhance
		// performances)
		tablePerName.values().forEach(table -> {
			Set<ForeignKeyMetadata> foreignKeyMetadata = metadataReader.giveExportedKeys(catalog, schema, table.name);
			foreignKeyMetadata.forEach(row -> {
				ArrayList<Column> sourceColumns = new ArrayList<>();
				ArrayList<Column> targetColumns = new ArrayList<>();
				row.getColumns().forEach(duo -> {
					sourceColumns.add(columnCache.get(new Duo<>(row.getSourceTable().getTableName(), duo.getLeft())));
					targetColumns.add(columnCache.get(new Duo<>(row.getTargetTable().getTableName(), duo.getRight())));
				});
				// setting foreign key to owning table
				tablePerName.get(row.getSourceTable().getTableName())
						.addForeignKey(row.getName(),
								sourceColumns, tablePerName.get(row.getTargetTable().getTableName()), targetColumns);
			});
		});
		
		// Collecting indexes
		tablePerName.values().forEach(table -> {
			Set<IndexMetadata> foreignKeyMetadata = metadataReader.giveIndexes(catalog, schema, table.name);
			foreignKeyMetadata.forEach(row -> {
				Index index = result.addIndex(row.getName());
				index.setUnique(row.isUnique());
				row.getColumns().forEach(duo -> {
					Boolean aBoolean = duo.getRight();
					AscOrDesc ascOrDesc;
					if (aBoolean == null) {
						ascOrDesc = null;
					} else if (aBoolean) {
						ascOrDesc = AscOrDesc.ASC;
					} else {
						ascOrDesc = AscOrDesc.DESC;
					}
					index.addColumn(columnCache.get(new Duo<>(row.getTableName(), duo.getLeft())), ascOrDesc);
				});
			});
		});
		
		Set<ViewMetadata> viewMetadata = metadataReader.giveViews(catalog, schema, tableNamePattern);
		viewMetadata.forEach(row -> {
			View view = result.addView(row.getName());
			SortedSet<ColumnMetadata> columnMetadata = metadataReader.giveColumns(catalog, schema, row.getName());
			columnMetadata.forEach(c -> view.addColumn(c.getName(),
					c.getVendorType(), c.getSize(), c.getPrecision(),
					c.isNullable()));
		});
		
		completeSchema(result);
		
		return result;
	}
	
	protected void completeSchema(Schema result) {
	
	}
	
	public static class Schema {
		
		private final String name;
		
		private final Set<Table> tables = new HashSet<>();
		
		private final Set<Index> indexes = new HashSet<>();
		
		private final Set<View> views = new HashSet<>();
		
		public Schema(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public Set<Table> getTables() {
			return tables;
		}
		
		Table addTable(String name) {
			Table table = new Table(name);
			this.tables.add(table);
			return table;
		}
		
		public Set<Index> getIndexes() {
			return indexes;
		}
		
		Index addIndex(String name) {
			Index result = new Index(name);
			this.indexes.add(result);
			return result;
		}
		
		public Set<View> getViews() {
			return views;
		}
		
		View addView(String name) {
			View result = new View(name);
			this.views.add(result);
			return result;
		}
		
		protected class Table {
			
			private final String name;
			private final List<Column> columns = new ArrayList<>();
			private PrimaryKey primaryKey;
			private final Set<ForeignKey> foreignKeys = new HashSet<>();
			private String comment;
			
			private Table(String name) {
				this.name = name;
			}
			
			Schema getSchema() {
				return Schema.this;
			}
			
			public List<Column> getColumns() {
				return columns;
			}
			
			public PrimaryKey getPrimaryKey() {
				return primaryKey;
			}
			
			void setPrimaryKey(List<Column> columns) {
				this.primaryKey = new PrimaryKey(columns);
			}
			
			public Set<ForeignKey> getForeignKeys() {
				return foreignKeys;
			}
			
			void addForeignKey(String name, List<Column> columns, Table targetTable, List<Column> targetColumns) {
				this.foreignKeys.add(new ForeignKey(name, columns, targetTable, targetColumns));
			}
			
			Column addColumn(String name, String type, Integer size, Integer precision, boolean nullable, boolean autoIncrement) {
				Column column = new Column(name, type, size, precision, nullable, autoIncrement);
				this.columns.add(column);
				return column;
			}
			
			public String getName() {
				return name;
			}
			
			public void setComment(String comment) {
				this.comment = comment;
			}
			
			public String getComment() {
				return comment;
			}
			
			@Override
			public String toString() {
				return "Table{" +
						"name='" + name + '\'' +
						'}';
			}
			
			protected class Column {
				
				private final String name;
				private final String type;
				private final Integer size;
				private final Integer precision;
				private final boolean nullable;
				private final boolean autoIncrement;
				
				private Column(String name, String type, Integer size, Integer precision, boolean nullable, boolean autoIncrement) {
					this.name = name;
					this.type = type;
					this.size = size;
					this.precision = precision;
					this.nullable = nullable;
					this.autoIncrement = autoIncrement;
				}
				
				public Table getTable() {
					return Table.this;
				}
				
				public String getName() {
					return name;
				}
				
				public String getType() {
					return type;
				}
				
				public Integer getSize() {
					return size;
				}
				
				@Override
				public String toString() {
					return "Column{" +
							"name='" + name + '\'' +
							", type='" + type + '\'' +
							", size=" + size +
							'}';
				}
			}
			
			protected class PrimaryKey {
				
				private final List<Column> columns;
				
				private PrimaryKey(List<Column> columns) {
					this.columns = columns;
				}
				
				public List<Column> getColumns() {
					return columns;
				}
				
				@Override
				public String toString() {
					return "PrimaryKey{" +
							"columns=" + columns +
							'}';
				}
			}
			
			protected class ForeignKey {
				
				private final String name;
				private final List<Column> columns;
				private final Table targetTable;
				private final List<Column> targetColumns;
				
				private ForeignKey(String name, List<Column> columns, Table targetTable, List<Column> targetColumns) {
					this.name = name;
					this.columns = columns;
					this.targetTable = targetTable;
					this.targetColumns = targetColumns;
				}
				
				public String getName() {
					return name;
				}
				
				public List<Column> getColumns() {
					return columns;
				}
				
				public Table getTargetTable() {
					return targetTable;
				}
				
				public List<Column> getTargetColumns() {
					return targetColumns;
				}
				
				@Override
				public String toString() {
					return "ForeignKey{" +
							"'" + name + '\'' + ": " +
							Iterables.collectToList(() -> new PairIterator<>(columns, targetColumns),
									duo -> duo.getLeft().getName() + "=>" + targetTable.getName() + "." + duo.getRight().getName()).toString() +
							'}';
				}
			}
		}
		
		protected class Index {
			
			private final String name;
			
			private boolean unique;
			
			/**
			 * TYPE short => index type:
			 * - tableIndexStatistic : this identifies table statistics that are returned in conjunction with a table's index descriptions
			 * - tableIndexClustered : this is a clustered index
			 * - tableIndexHashed : this is a hashed index
			 * - tableIndexOther : this is some other style of index
			 */
			private short type;
			
			private String filterCondition;
			
			private final KeepOrderMap<Column, AscOrDesc> columns = new KeepOrderMap<>();
			
			private String indexQualifier;
			
			protected Index(String name) {
				this.name = name;
			}
			
			public String getName() {
				return name;
			}
			
			public boolean isUnique() {
				return unique;
			}
			
			public void setUnique(boolean unique) {
				this.unique = unique;
			}
			
			public void setType(short type) {
				this.type = type;
			}
			
			public void setFilterCondition(String filterCondition) {
				this.filterCondition = filterCondition;
			}
			
			public KeepOrderMap<Column, AscOrDesc> getColumns() {
				return columns;
			}
			
			void addColumn(Column column, AscOrDesc ascOrDesc) {
				this.columns.put(column, ascOrDesc);
			}
			
			public void setIndexQualifier(String indexQualifier) {
				this.indexQualifier = indexQualifier;
			}
		}
		
		enum AscOrDesc {
			ASC,
			DESC
		}
		
		protected class View {
			
			private final String name;
			private final KeepOrderSet<PseudoColumn> columns = new KeepOrderSet<>();
			
			protected View(String name) {
				this.name = name;
			}
			
			Schema getSchema() {
				return Schema.this;
			}
			
			public String getName() {
				return name;
			}
			
			View addColumn(String name, String type, Integer size, Integer precision, boolean nullable) {
				PseudoColumn column = new PseudoColumn(name, type, size, precision, nullable);
				this.columns.add(column);
				return this;
			}
			
			public Set<PseudoColumn> getColumns() {
				return this.columns;
			}
			
			protected class PseudoColumn {
				
				private final String name;
				private final String type;
				private final Integer size;
				private final Integer precision;
				private final boolean nullable;
				
				private PseudoColumn(String name, String type, Integer size, Integer precision, boolean nullable) {
					this.name = name;
					this.type = type;
					this.size = size;
					this.precision = precision;
					this.nullable = nullable;
				}
				
				View getView() {
					return View.this;
				}
				
				public String getName() {
					return name;
				}
				
				public String getType() {
					return type;
				}
				
				public Integer getSize() {
					return size;
				}
				
				public Integer getPrecision() {
					return precision;
				}
				
				public boolean isNullable() {
					return nullable;
				}
				
				@Override
				public String toString() {
					return "Column{" +
							"name='" + name + '\'' +
							", type='" + type + '\'' +
							", size=" + size +
							'}';
				}
			}
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
