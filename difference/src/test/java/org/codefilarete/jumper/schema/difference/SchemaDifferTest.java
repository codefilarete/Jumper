package org.codefilarete.jumper.schema.difference;

import java.sql.JDBCType;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.ForeignKey;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.PrimaryKey;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.UniqueConstraint;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.View;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator.PropertyDiff;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Predicates;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.tool.collection.Arrays.asHashSet;

class SchemaDifferTest {
	
	/**
	 * Comparator for {@link SerializableFunction} to compare {@link PropertyDiff#propertyAccessor}, else AssertJ compares
	 * their fields, and since there is no field in {@link SerializableFunction}, it returns always true, making impossible
	 * to fail test, therefore impossible to detect a mistake.
	 */
	private static final Comparator<SerializableFunction> SERIALIZABLE_FUNCTION_COMPARATOR = Predicates.toComparator((SerializableFunction f1, SerializableFunction f2) ->
			AccessorDefinition.giveDefinition(Accessors.accessor(f1)).equals(AccessorDefinition.giveDefinition(Accessors.accessor(f2))));
	public static final RecursiveComparisonConfiguration RECURSIVE_COMPARISON_CONFIGURATION = RecursiveComparisonConfiguration.builder()
			.withComparatorForType(SERIALIZABLE_FUNCTION_COMPARATOR, SerializableFunction.class)
			.build();
	
	@Test
	void compare_emptySchema_returnsNoDifferences() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Set<AbstractDiff<?>> compare = testInstance.compare(new Schema("schema1"), new Schema("schema2"));
		
		assertThat(compare).isEmpty();
	}
	
	@Test
	void compare_schemasWithExtraTableAndMissingOne_returnsTableDifferences() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		schema1.addTable("DummyTable");
		Table missingTable = schema1.addTable("MissingTable");
		Schema schema2 = new Schema("schema2");
		schema2.addTable("DummyTable");
		Table extraTable = schema2.addTable("ExtraTable");
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		Diff<Table> expectedDiff1 = new Diff<>(State.REMOVED, missingTable, null);
		Diff<Table> expectedDiff2 = new Diff<>(State.ADDED, null, extraTable);
		Arrays.asSet(expectedDiff1, expectedDiff2).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(Schema.class, "tables", Set.class)));
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator()	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(expectedDiff1, expectedDiff2);
	}
	
	@Test
	void compare_schemasWithAnExtraUniqueConstraint_returnTheExtraUniqueConstraint() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		Table dummyTable1 = schema1.addTable("DummyTable");
		Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column otherDummyColumn1 = dummyTable1.addColumn("otherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Schema schema2 = new Schema("schema2");
		Table dummyTable2 = schema2.addTable("DummyTable");
		Column dummyColumn2 = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column otherDummyColumn2 = dummyTable2.addColumn("otherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		UniqueConstraint uniqueConstraint = dummyTable2.addUniqueConstraint("my_constraint", dummyColumn2);
		
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(
						new Diff<>(State.ADDED, null, uniqueConstraint)
				);
	}
	
	@Test
	void compare_schemasWithAnUniqueConstraint_differingOnColumn_returnUniqueConstraints() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		Table dummyTable1 = schema1.addTable("DummyTable");
		Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column otherDummyColumn1 = dummyTable1.addColumn("otherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		UniqueConstraint uniqueConstraint1 = dummyTable1.addUniqueConstraint("my_constraint", dummyColumn1, otherDummyColumn1);
		
		Schema schema2 = new Schema("schema2");
		Table dummyTable2 = schema2.addTable("DummyTable");
		Column dummyColumn2 = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column otherDummyColumn2 = dummyTable2.addColumn("otherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		UniqueConstraint uniqueConstraint2 = dummyTable2.addUniqueConstraint("my_constraint", dummyColumn2);
		
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(
						// from a unique constraint columns point of view (the key in the SchemaDiffer), the unique constraints are very different
						new Diff<>(State.REMOVED, uniqueConstraint1, null),
						new Diff<>(State.ADDED, null, uniqueConstraint2)
				);
	}
	
	@Test
	void compare_tablesWithColumnDifference_returnsColumnDifferences() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		Table dummyTable1 = schema1.addTable("DummyTable");
		dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column missingColumn = dummyTable1.addColumn("missingColumn", JDBCType.BIGINT, 42, 12, true, true);
		Schema schema2 = new Schema("schema2");
		Table dummyTable2 = schema2.addTable("DummyTable");
		dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column extraColumn = dummyTable2.addColumn("extraColumn", JDBCType.BIGINT, 42, 12, true, true);
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		IndexedDiff<Column> expectedDiff1 = new IndexedDiff<>(State.REMOVED, missingColumn, null, asHashSet(1), asHashSet());
		IndexedDiff<Column> expectedDiff2 = new IndexedDiff<>(State.ADDED, null, extraColumn, asHashSet(), asHashSet(1));
		Arrays.asSet(expectedDiff1, expectedDiff2).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(Table.class, "columns", List.class)));
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator()	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(expectedDiff1, expectedDiff2);
	}
	
	@Test
	void compare_tablesWithPrimaryKeyDifference_returnsPrimaryKeyDifferences() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		Table dummyTable1 = schema1.addTable("DummyTable");
		Column column = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		PrimaryKey primary = dummyTable1.setPrimaryKey("PRIMARY", Arrays.asList(column));
		Schema schema2 = new Schema("schema2");
		Table dummyTable2 = schema2.addTable("DummyTable");
		dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(
						new PropertyDiff<>(Table::getPrimaryKey, dummyTable1, dummyTable2));
	}
	
	
	@Test
	void compare_tablesWithPrimaryKeyDifference_returnsPrimaryKeyDifferences2() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		Table dummyTable1 = schema1.addTable("DummyTable");
		Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		dummyTable1.addColumn("otherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		PrimaryKey primaryKey1 = dummyTable1.setPrimaryKey("PRIMARY", Arrays.asList(dummyColumn1));
		Schema schema2 = new Schema("schema2");
		Table dummyTable2 = schema2.addTable("DummyTable");
		Column dummyColumn2 = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		Column otherDummyColumn = dummyTable2.addColumn("otherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		PrimaryKey primaryKey2 = dummyTable2.setPrimaryKey("PRIMARY", Arrays.asList(otherDummyColumn));
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		
		IndexedDiff<Column> expectedDiff1 = new IndexedDiff<>(State.REMOVED, dummyColumn1, null, asHashSet(0), asHashSet());
		IndexedDiff<Column> expectedDiff2 = new IndexedDiff<>(State.ADDED, null, otherDummyColumn, asHashSet(), asHashSet(0));
		Arrays.asSet(expectedDiff1, expectedDiff2).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(PrimaryKey.class, "columns", List.class)));
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(expectedDiff1, expectedDiff2);
	}
	
	@Test
	void compare_foreignKeys_missing() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		ForeignKey myForeignKey;
		{
			Table dummyTable1 = schema1.addTable("DummyTable1");
			Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Table dummyTable2 = schema1.addTable("DummyTable2");
			Column dummyColumn2 = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			myForeignKey = dummyTable1.addForeignKey("my_foreignKey", Arrays.asList(dummyColumn1), dummyTable2, Arrays.asList(dummyColumn2));
		}
		Schema schema2 = new Schema("schema2");
		{
			Table dummyTable1 = schema2.addTable("DummyTable1");
			Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Table dummyTable2 = schema2.addTable("DummyTable2");
			Column dummyColumn2 = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
		}
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator()	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(
						new Diff<>(State.REMOVED, myForeignKey, null));
	}
	
	@Test
	void compare_foreignKeys_differing() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		Column dummyColumn2;
		{
			Table dummyTable1 = schema1.addTable("DummyTable1");
			Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Table dummyTable2 = schema1.addTable("DummyTable2");
			dummyColumn2 = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			ForeignKey myForeignKey1 = dummyTable1.addForeignKey("my_foreignKey", Arrays.asList(dummyColumn1), dummyTable2, Arrays.asList(dummyColumn2));
		}
		Schema schema2 = new Schema("schema2");
		Column yetAnotherDummyColumn2;
		{
			Table dummyTable1 = schema2.addTable("DummyTable1");
			Column dummyColumn1 = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Table dummyTable2 = schema2.addTable("DummyTable2");
			dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			yetAnotherDummyColumn2 = dummyTable2.addColumn("yetAnotherDummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			ForeignKey myForeignKey2 = dummyTable1.addForeignKey("my_foreignKey", Arrays.asList(dummyColumn1), dummyTable2, Arrays.asList(yetAnotherDummyColumn2));
		}
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		IndexedDiff<Column> expectedDiff1 = new IndexedDiff<>(State.ADDED, null, yetAnotherDummyColumn2, asHashSet(), asHashSet(0));
		IndexedDiff<Column> expectedDiff2 = new IndexedDiff<>(State.REMOVED, dummyColumn2, null, asHashSet(0), asHashSet());
		Arrays.asSet(expectedDiff1, expectedDiff2).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(ForeignKey.class, "targetColumns", List.class)));
		IndexedDiff<Column> expectedDiff3 = new IndexedDiff<>(State.ADDED, null, yetAnotherDummyColumn2, asHashSet(), asHashSet(1));
		Arrays.asSet(expectedDiff3).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(Table.class, "columns", List.class)));
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator()	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(expectedDiff1, expectedDiff2, expectedDiff3);
	}
	
	@Nested
	class CompareColumnDifference {
		
		@Test
		void typeDifference() {
			SchemaDiffer testInstance = new SchemaDiffer();
			Schema schema1 = new Schema("schema1");
			Table dummyTable1 = schema1.addTable("DummyTable");
			Column expectedColumn = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Schema schema2 = new Schema("schema2");
			Table dummyTable2 = schema2.addTable("DummyTable");
			Column actualColumn = dummyTable2.addColumn("dummyColumn", JDBCType.BOOLEAN, 42, 12, true, true);
			Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
			
			assertThat(compare)
					.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)
					.containsExactlyInAnyOrder(
							new PropertyDiff<>(Column::getType, expectedColumn, actualColumn));
		}
		
		@Test
		void sizeDifference() {
			SchemaDiffer testInstance = new SchemaDiffer();
			Schema schema1 = new Schema("schema1");
			Table dummyTable1 = schema1.addTable("DummyTable");
			Column expectedColumn = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Schema schema2 = new Schema("schema2");
			Table dummyTable2 = schema2.addTable("DummyTable");
			Column actualColumn = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 666, 12, true, true);
			Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
			
			assertThat(compare)
					.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)
					.containsExactlyInAnyOrder(
							new PropertyDiff<>(Column::getSize, expectedColumn, actualColumn));
		}
		
		@Test
		void precisionDifference() {
			SchemaDiffer testInstance = new SchemaDiffer();
			Schema schema1 = new Schema("schema1");
			Table dummyTable1 = schema1.addTable("DummyTable");
			Column expectedColumn = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Schema schema2 = new Schema("schema2");
			Table dummyTable2 = schema2.addTable("DummyTable");
			Column actualColumn = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 666, true, true);
			Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
			
			assertThat(compare)
					.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)
					.containsExactlyInAnyOrder(
							new PropertyDiff<>(Column::getScale, expectedColumn, actualColumn));
		}
		
		@Test
		void nullableDifference() {
			SchemaDiffer testInstance = new SchemaDiffer();
			Schema schema1 = new Schema("schema1");
			Table dummyTable1 = schema1.addTable("DummyTable");
			Column expectedColumn = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Schema schema2 = new Schema("schema2");
			Table dummyTable2 = schema2.addTable("DummyTable");
			Column actualColumn = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, false, true);
			Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
			
			assertThat(compare)
					.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)
					.containsExactlyInAnyOrder(
							new PropertyDiff<>(Column::isNullable, expectedColumn, actualColumn));
		}
		
		@Test
		void autoIncrementDifference() {
			SchemaDiffer testInstance = new SchemaDiffer();
			Schema schema1 = new Schema("schema1");
			Table dummyTable1 = schema1.addTable("DummyTable");
			Column expectedColumn = dummyTable1.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, true);
			Schema schema2 = new Schema("schema2");
			Table dummyTable2 = schema2.addTable("DummyTable");
			Column actualColumn = dummyTable2.addColumn("dummyColumn", JDBCType.BIGINT, 42, 12, true, false);
			Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
			
			assertThat(compare)
					.usingRecursiveFieldByFieldElementComparator(RECURSIVE_COMPARISON_CONFIGURATION)
					.containsExactlyInAnyOrder(
							new PropertyDiff<>(Column::isAutoIncrement, expectedColumn, actualColumn));
		}
	}
	
	@Test
	void compare_schemasWithExtraIndexAndMissingOne_returnsIndexDifferences() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		schema1.addIndex("DummyIndex");
		Index missingIndex = schema1.addIndex("MissingIndex");
		Schema schema2 = new Schema("schema2");
		schema2.addIndex("DummyIndex");
		Index extraIndex = schema2.addIndex("ExtraIndex");
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		Diff<Index> expectedDiff1 = new Diff<>(State.REMOVED, missingIndex, null);
		Diff<Index> expectedDiff2 = new Diff<>(State.ADDED, null, extraIndex);
		Arrays.asSet(expectedDiff1, expectedDiff2).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(Schema.class, "indexes", Set.class)));
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator()	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(expectedDiff1, expectedDiff2);
	}
	
	@Test
	void compare_schemasWithExtraViewAndMissingOne_returnsViewDifferences() {
		SchemaDiffer testInstance = new SchemaDiffer();
		Schema schema1 = new Schema("schema1");
		schema1.addView("DummyView");
		View missingView = schema1.addView("MissingView");
		Schema schema2 = new Schema("schema2");
		schema2.addView("DummyView");
		View extraView = schema2.addView("ExtraView");
		Set<AbstractDiff<?>> compare = testInstance.compare(schema1, schema2);
		
		
		Diff<View> expectedDiff1 = new Diff<>(State.REMOVED, missingView, null);
		Diff<View> expectedDiff2 = new Diff<>(State.ADDED, null, extraView);
		Arrays.asSet(expectedDiff1, expectedDiff2).forEach(diff -> diff.setCollectionAccessor(new AccessorDefinition(Schema.class, "views", Set.class)));
		
		assertThat(compare)
				.usingRecursiveFieldByFieldElementComparator()	// because Diff class doesn't implement equals() and we don't want it to
				.containsExactlyInAnyOrder(expectedDiff1, expectedDiff2);
	}
	
}