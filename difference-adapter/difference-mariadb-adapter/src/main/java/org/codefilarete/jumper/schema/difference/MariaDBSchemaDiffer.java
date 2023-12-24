package org.codefilarete.jumper.schema.difference;

import java.util.Map.Entry;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.AscOrDesc;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;

public class MariaDBSchemaDiffer extends SchemaDiffer {
	
	@Override
	protected ComparisonChain<Schema> configure() {
		return comparisonChain(Schema.class)
				.compareOn(Schema::getTables, Table::getName, comparisonChain(Table.class)
						.compareOn(Table::getColumns, Column::getName, comparisonChain(Column.class)
								.compareOn(Column::getType)
								.compareOn(Column::getSize)
								.compareOn(Column::getPrecision)
								.compareOn(Column::isNullable)
								.compareOn(Column::isAutoIncrement))
						.compareOn(Table::getComment))
				.compareOn(Schema::getIndexes, Index::getName, comparisonChain(Index.class)
						.compareOn(Index::isUnique)
						// no need to compare on ascendant or descendant direction since MariaDB doesn't support it
						.compareOnMap(Index::getColumns, Column::getName,
								// Derby is sensitive to Index direction thus we add comparison on it
								comparisonChain((Class<Entry<Column, AscOrDesc>>) (Class) Entry.class)
										.compareOn(Entry::getValue))
				);
	}
}
