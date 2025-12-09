package org.codefilarete.jumper.schema.difference;

import java.util.Map.Entry;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.AscOrDesc;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Indexable;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;

public class H2SchemaDiffer extends SchemaDiffer {
	
	@Override
	protected ComparisonChain<Schema> configure() {
		return comparisonChain(Schema.class)
				.compareOn(Schema::getName)
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
								comparisonChain((Class<Entry<Indexable, AscOrDesc>>) (Class) Entry.class)
								.compareOn(Entry::getValue))
				);
	}
}
