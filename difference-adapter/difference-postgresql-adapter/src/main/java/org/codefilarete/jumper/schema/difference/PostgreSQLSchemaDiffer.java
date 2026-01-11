package org.codefilarete.jumper.schema.difference;

import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index.IndexedColumn;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Indexable;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.ForeignKey;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.UniqueConstraint;

public class PostgreSQLSchemaDiffer extends SchemaDiffer {
	
	@Override
	protected ComparisonChain<Schema> configure() {
		return comparisonChain(Schema.class)
				.compareOn(Schema::getTables, Table::getName, comparisonChain(Table.class)
						.compareOn(Table::getColumns, Column::getName, comparisonChain(Column.class)
								.compareOn(Column::getType)
								.compareOn(Column::getSize)
								.compareOn(Column::getScale)
								.compareOn(Column::isNullable)
								.compareOn(Column::isAutoIncrement))
						.compareOn(Table::getComment))
				.compareOn(Schema::getIndexes, Index::getName, comparisonChain(Index.class)
						.compareOn(Index::isUnique)
						.compareOn(Index::getColumns, indexedColumn -> indexedColumn.getColumn().getName(), comparisonChain(IndexedColumn.class)
								// PostgreSQL is sensitive to Index direction thus we add comparison on it
								.compareOn(IndexedColumn::getDirection)))
				.compareOn(schema -> schema.getTables().stream().flatMap(t -> t.getForeignKeys().stream()).collect(Collectors.toSet()),
						"Foreign keys",
						fk -> fk.getColumns().stream().map(c -> c.getTable().getName()+ "." + c.getName()).collect(Collectors.joining(", ")),
						comparisonChain(ForeignKey.class)
								.compareOn(ForeignKey::getColumns, Column::getName)
								.compareOn(ForeignKey::getTargetColumns, Column::getName)
								.compareOn(ForeignKey::getName))
				.compareOn(schema -> schema.getTables().stream().flatMap(t -> t.getUniqueConstraints().stream()).collect(Collectors.toSet()),
						"Unique Constraints",
						uk -> uk.getColumns().stream().map(c -> c.getTable().getName()+ "." + c.getName()).collect(Collectors.joining(", ")),
						comparisonChain(UniqueConstraint.class)
								.compareOn(UniqueConstraint::getColumns, Indexable::getName)
								.compareOn(UniqueConstraint::getName))
				;
	}
}
