package org.codefilarete.jumper.schema;

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.AscOrDesc;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Indexable;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.PrimaryKey;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.View;
import org.codefilarete.jumper.schema.metadata.ColumnMetadata;
import org.codefilarete.jumper.schema.metadata.DefaultMetadataReader;
import org.codefilarete.jumper.schema.metadata.ForeignKeyMetadata;
import org.codefilarete.jumper.schema.metadata.IndexMetadata;
import org.codefilarete.jumper.schema.metadata.SchemaMetadataReader;
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

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Collects database schema elements based on a {@link SchemaMetadataReader} to read DDL elements and build a structured
 * version of them through a {@link Schema}.
 *
 * @see #collect()
 * @author Guillaume Mary
 */
public class DefaultSchemaElementCollector extends SchemaElementCollector {
	
	/**
	 * Constructor to create schema elements from given {@link DatabaseMetaData}.
	 * Will use a {@link DefaultMetadataReader} to collect elements and create a {@link Schema} on {@link #collect()}
	 * call.
	 *
	 * @param databaseMetaData
	 */
	public DefaultSchemaElementCollector(DatabaseMetaData databaseMetaData) {
		this(new DefaultMetadataReader(databaseMetaData));
	}
	
	/**
	 * Generic constructor. Will create {@link Schema} elements from given {@link SchemaMetadataReader} on {@link #collect()}
	 * call.
	 *
	 * @param metadataReader
	 */
	public DefaultSchemaElementCollector(SchemaMetadataReader metadataReader) {
		super(metadataReader);
	}
	
	@Override
	public DefaultSchemaElementCollector withCatalog(String catalog) {
		super.withCatalog(catalog);
		return this;
	}
	
	@Override
	public DefaultSchemaElementCollector withSchema(String schema) {
		super.withSchema(schema);
		return this;
	}
	
	@Override
	public DefaultSchemaElementCollector withTableNamePattern(String tableNamePattern) {
		super.withTableNamePattern(tableNamePattern);
		return this;
	}
	
	public Schema collect() {
		Schema result = createSchema(buildNameSpace());
		
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
		Set<ColumnMetadata> columnMetadata = metadataReader.giveColumns(catalog, schema, tableNamePattern);
		columnMetadata.stream().filter(c -> tablePerName.containsKey(c.getTableName()))
				.sorted(Comparator.comparing(ColumnMetadata::getTableName).thenComparing(ColumnMetadata::getPosition)).forEach(row -> {
			Table table = tablePerName.get(row.getTableName());
			Column column = table.addColumn(row.getName(),
					row.getSqlType(), row.getSize(), row.getPrecision(),
					row.isNullable(), row.isAutoIncrement());
			columnCache.put(new Duo<>(table.getName(), row.getName()), column);
		});
		
		Set<PrimaryKeyMetadata> primaryKeyMetadata = metadataReader.givePrimaryKey(catalog, schema, tableNamePattern);
		primaryKeyMetadata.forEach(primaryKeyMetadatum -> {
			List<Column> primaryKeyColumns = new ArrayList<>();
			primaryKeyMetadatum.getColumns().forEach(columnName -> {
				primaryKeyColumns.add(columnCache.get(new Duo<>(primaryKeyMetadatum.getTableName(), columnName)));
			});
			tablePerName.get(primaryKeyMetadatum.getTableName()).setPrimaryKey(primaryKeyMetadatum.getName(), primaryKeyColumns);
		});

		// Collecting Foreign Keys
		Set<ForeignKeyMetadata> foreignKeyMetadata = metadataReader.giveExportedKeys(catalog, schema, tableNamePattern);
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
		
		// Collecting indexes
		Set<IndexMetadata> indexesMetadata = metadataReader.giveIndexes(catalog, schema, tableNamePattern);
		Map<String, List<IndexMetadata>> indexPerTableName = indexesMetadata.stream().collect(Collectors.groupingBy(IndexMetadata::getTableName));
		tablePerName.keySet().forEach(tableName -> {
			List<IndexMetadata> tableIndexes = indexPerTableName.get(tableName);
			if (tableIndexes != null) {
				tableIndexes.forEach(indexMetadata -> {
					boolean addIndex = shouldAddIndex(result, indexMetadata);
					if (addIndex) {
						Index index = result.addIndex(indexMetadata.getName());
						index.setUnique(indexMetadata.isUnique());
						indexMetadata.getColumns().forEach(duo -> {
							AscOrDesc direction = mapBooleanToDirection(duo.getRight());
							Indexable column = columnCache.get(new Duo<>(indexMetadata.getTableName(), duo.getLeft()));
							if (column == null) {
								// The indexed value is not a column but an expression (a column substring for example) so we create a special object for it
								Table indexTable = Iterables.find(result.getTables(), table -> table.getName().equals(indexMetadata.getTableName()));
								column = new Indexable() {
									@Override
									public String getName() {
										return duo.getLeft();
									}
									
									@Override
									public Table getTable() {
										return indexTable;
									}
								};
							}
							index.addColumn(column, direction);
						});
					}
				});
			}
		});
		
		Set<ViewMetadata> viewMetadata = metadataReader.giveViews(catalog, schema, tableNamePattern);
		viewMetadata.forEach(row -> {
			View view = result.addView(row.getName());
			SortedSet<ColumnMetadata> viewColumnMetadata = metadataReader.giveColumns(catalog, schema, row.getName());
			viewColumnMetadata.forEach(c -> view.addColumn(c.getName(),
					c.getSqlType(), c.getSize(), c.getPrecision(),
					c.isNullable()));
		});
		
		completeSchema(result);
		
		return result;
	}
	
	private String buildNameSpace() {
		StringAppender namespace = new StringAppender();
		String catalogName = nullable(super.catalog).getOr(() -> {
			try {
				return metadataReader.getMetaData().getConnection().getCatalog();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		namespace.catIf(!Strings.isEmpty(catalogName), catalogName);
		String schemaName = nullable(super.schema).getOr(() -> {
			try {
				return metadataReader.getMetaData().getConnection().getSchema();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		if (!Strings.isEmpty(schemaName)) {
			namespace.catIf(namespace.length() != 0, ".").cat(schemaName);
		}
		return namespace.toString();
	}
	
	private static AscOrDesc mapBooleanToDirection(Boolean ascOrDesc) {
		AscOrDesc direction;
		if (ascOrDesc == null) {
			direction = null;
		} else if (ascOrDesc) {
			direction = AscOrDesc.ASC;
		} else {
			direction = AscOrDesc.DESC;
		}
		return direction;
	}
	
	protected boolean shouldAddIndex(Schema result, IndexMetadata indexMetadata) {
		// we don't take into account indexes that matches primaryKey names because some database vendors
		// create one unique index per primaryKey to implement it, so those indexes are considered "noise"
		// as they are highly tied to primaryKey presence (can't be deleted without removing primaryKey)
		Optional<String> matchingPkName = result.getTables().stream().map(t -> nullable(t.getPrimaryKey()).map(PrimaryKey::getName).getOr((String) null))
				.filter(pkName -> indexMetadata.getName().equals(pkName)).findFirst();
		return !matchingPkName.isPresent();
	}
	
	protected Schema createSchema(String schemaName) {
		return new Schema(Strings.preventEmpty(schemaName, null));
	}
	
	protected void completeSchema(Schema result) {
	
	}
	
	public interface SchemaElement {
		
		Schema getSchema();
	}
	
	public interface TableElement {
		
		Table getTable();
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
		
		public Table addTable(String name) {
			Table table = new Table(name);
			this.tables.add(table);
			return table;
		}
		
		public Set<Index> getIndexes() {
			return indexes;
		}
		
		public Index addIndex(String name) {
			Index result = new Index(name);
			this.indexes.add(result);
			return result;
		}
		
		public Set<View> getViews() {
			return views;
		}
		
		public View addView(String name) {
			View result = new View(name);
			this.views.add(result);
			return result;
		}
		
		@Override
		public String toString() {
			return "Schema{" +
					"name='" + name + '\'' +
					'}';
		}
		
		public class Table implements SchemaElement {
			
			private final String name;
			private final List<Column> columns = new ArrayList<>();
			private PrimaryKey primaryKey;
			private final Set<ForeignKey> foreignKeys = new HashSet<>();
			private String comment;
			
			protected Table(String name) {
				this.name = name;
			}
			
			@Override
			public Schema getSchema() {
				return Schema.this;
			}
			
			public List<Column> getColumns() {
				return columns;
			}
			
			public Column addColumn(String name, JDBCType type, Integer size, Integer precision, boolean nullable, boolean autoIncrement) {
				Column column = new Column(name, type, size, precision, nullable, autoIncrement);
				this.columns.add(column);
				return column;
			}
			
			public PrimaryKey getPrimaryKey() {
				return primaryKey;
			}
			
			public PrimaryKey setPrimaryKey(String name, List<Column> columns) {
				this.primaryKey = new PrimaryKey(name, columns);
				return primaryKey;
			}
			
			public Set<ForeignKey> getForeignKeys() {
				return foreignKeys;
			}
			
			public void addForeignKey(String name, List<Column> columns, Table targetTable, List<Column> targetColumns) {
				this.foreignKeys.add(new ForeignKey(name, columns, targetTable, targetColumns));
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
			
			public class Column implements SchemaElement, TableElement, Indexable {
				
				private final String name;
				private final JDBCType type;
				private final Integer size;
				private final Integer scale;
				private final boolean nullable;
				private final boolean autoIncrement;
				
				protected Column(String name, JDBCType type, Integer size, Integer scale, boolean nullable, boolean autoIncrement) {
					this.name = name;
					this.type = type;
					this.size = size;
					this.scale = scale;
					this.nullable = nullable;
					this.autoIncrement = autoIncrement;
				}
				
				@Override
				public Schema getSchema() {
					return getTable().getSchema();
				}
				
				@Override
				public Table getTable() {
					return Table.this;
				}
				
				@Override
				public String getName() {
					return name;
				}
				
				public JDBCType getType() {
					return type;
				}
				
				public Integer getSize() {
					return size;
				}
				
				public Integer getScale() {
					return scale;
				}
				
				public boolean isNullable() {
					return nullable;
				}
				
				public boolean isAutoIncrement() {
					return autoIncrement;
				}
				
				@Override
				public String toString() {
					return "Column{" +
							"tableName='" + getTable().getName() + '\'' +
							", name='" + name + '\'' +
							", type='" + type + '\'' +
							", size=" + size +
							", scale=" + scale +
							", nullable=" + nullable +
							'}';
				}
			}
			
			public class PrimaryKey implements SchemaElement, TableElement {
				
				private final String name;
				private final List<Column> columns;
				
				private PrimaryKey(String name, List<Column> columns) {
					this.name = name;
					this.columns = columns;
				}
				
				@Override
				public Schema getSchema() {
					return Schema.this;
				}
				
				@Override
				public Table getTable() {
					return Table.this;
				}
				
				public String getName() {
					return name;
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
			
			public class ForeignKey implements SchemaElement, TableElement {
				
				private final String name;
				private final List<Column> columns;
				private final Table targetTable;
				private final List<Column> targetColumns;
				
				protected ForeignKey(String name, List<Column> columns, Table targetTable, List<Column> targetColumns) {
					this.name = name;
					this.columns = columns;
					this.targetTable = targetTable;
					this.targetColumns = targetColumns;
				}
				
				@Override
				public Schema getSchema() {
					return Schema.this;
				}
				
				@Override
				public Table getTable() {
					return Table.this;
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
		
		public class Index implements SchemaElement, TableElement {
			
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
			
			private final KeepOrderMap<Indexable, AscOrDesc> columns = new KeepOrderMap<>();
			
			private String indexQualifier;
			
			protected Index(String name) {
				this.name = name;
			}
			
			@Override
			public Schema getSchema() {
				return Schema.this;
			}
			
			@Override
			public Table getTable() {
				return Iterables.first(columns.keySet()).getTable();
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
			
			public KeepOrderMap<Indexable, AscOrDesc> getColumns() {
				return columns;
			}
			
			public void addColumn(Indexable column, AscOrDesc ascOrDesc) {
				this.columns.put(column, ascOrDesc);
			}
			
			public void setIndexQualifier(String indexQualifier) {
				this.indexQualifier = indexQualifier;
			}
			
			@Override
			public String toString() {
				return "Index{" +
						"name='" + name + '\'' +
						", table='" + getTable().getName() + '\''+
						", unique=" + unique +
						", columns={" + columns.keySet().stream().map(Indexable::getName).map(s -> '\'' + s + '\'').collect(Collectors.joining(", ")) + "}" +
						'}';
			}
		}
		
		public interface Indexable {
			
			String getName();
			
			Table getTable();
			
		}
		
		public enum AscOrDesc {
			ASC,
			DESC
		}
		
		public class View implements SchemaElement {
			
			private final String name;
			private final KeepOrderSet<PseudoColumn> columns = new KeepOrderSet<>();
			
			protected View(String name) {
				this.name = name;
			}
			
			@Override
			public Schema getSchema() {
				return Schema.this;
			}
			
			public String getName() {
				return name;
			}
			
			View addColumn(String name, JDBCType type, Integer size, Integer precision, boolean nullable) {
				PseudoColumn column = new PseudoColumn(name, type, size, precision, nullable);
				this.columns.add(column);
				return this;
			}
			
			public Set<PseudoColumn> getColumns() {
				return this.columns;
			}
			
			public class PseudoColumn {
				
				private final String name;
				private final JDBCType type;
				private final Integer size;
				private final Integer precision;
				private final boolean nullable;
				
				protected PseudoColumn(String name, JDBCType type, Integer size, Integer precision, boolean nullable) {
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
				
				public JDBCType getType() {
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
	}
}
