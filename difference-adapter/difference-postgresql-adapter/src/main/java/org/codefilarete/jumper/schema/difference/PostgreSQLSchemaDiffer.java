package org.codefilarete.jumper.schema.difference;

import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.AscOrDesc;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Indexable;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.ForeignKey;

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
						.compareOnMap(Index::getColumns, Indexable::getName,
								// PostgreSQL is sensitive to Index direction thus we add comparison on it
								comparisonChain((Class<Entry<Indexable, AscOrDesc>>) (Class) Entry.class)
										.compareOn(Entry::getValue)))
				.compareOn(schema -> schema.getTables().stream().flatMap(t -> t.getForeignKeys().stream()).collect(Collectors.toSet()),
						"Foreign keys",
						fk -> fk.getColumns().stream().map(c -> c.getTable().getName()+ "." + c.getName()).collect(Collectors.joining(", ")),
						comparisonChain(ForeignKey.class)
								.compareOn(ForeignKey::getColumns, Column::getName)
								.compareOn(ForeignKey::getTargetColumns, Column::getName)
								.compareOn(ForeignKey::getName)
				);
	}
}
