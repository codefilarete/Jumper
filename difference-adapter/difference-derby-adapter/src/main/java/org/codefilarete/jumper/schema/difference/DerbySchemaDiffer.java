package org.codefilarete.jumper.schema.difference;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index.IndexedColumn;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;

public class DerbySchemaDiffer extends SchemaDiffer {
	
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
						// Derby is sensitive to Index direction thus we add comparison on it
						.compareOn(Index::getColumns, indexedColumn -> indexedColumn.getColumn().getName(), comparisonChain(IndexedColumn.class)
								.compareOn(IndexedColumn::getDirection)))
				;
	}
}
