package org.codefilarete.jumper.schema.difference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.SchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.SchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator.PropertyDiff;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.danekja.java.util.function.serializable.SerializableFunction;

public class SchemaDiffer {
	
	Set<AbstractDiff<?>> compare(Schema schema1, Schema schema2) {
		ComparisonChain<Schema> comparisonChain = comparisonChain(Schema.class)
				.compareOn(Schema::getTables, Table::getName)
				.thenForeachHeld(comparisonChain(Table.class)
						.compareOn(Table::getComment)
						.compareOn(Table::getColumns, Column::getName)
						.thenForeachHeld(comparisonChain(Column.class)
								.compareOn(Column::getName)
								.compareOn(Column::getType)
								.compareOn(Column::getSize)
//								.compareOn(Column::getPrecision)
//								.compareOn(Column::isNullable)
//								.compareOn(Column::isAutoIncrement)
						)
				);
//				.compareOn(Schema::getIndexes, Index::getName);
		
		return comparisonChain.run(schema1, schema2);
	}
	
	<T> ComparisonChain<T> comparisonChain(Class<T> clazz) {
		return new ComparisonChain<>(clazz);
	}
	
	public static class ComparisonChain<T> {
		
		private final Class<T> comparedType;
		
		private final List<Object> propertiesToCompare = new ArrayList<>();
		
		public ComparisonChain(Class<T> comparedType) {
			this.comparedType = comparedType;
		}
		
		public <E, C extends Collection<E>> CollectionComparisonChain<E, C> compareOn(SerializableFunction<T, C> collectionAccessor, SerializableFunction<E, ?> keyAccessor) {
			CollectionComparisonChain<E, C> collectionComparison = new CollectionComparisonChain<>(collectionAccessor, keyAccessor);
			this.propertiesToCompare.add(collectionComparison);
			return collectionComparison;
		}
		
		public <O> ComparisonChain<T> compareOn(SerializableFunction<T, O> propertyAccessor) {
			this.propertiesToCompare.add(new PropertyComparator<>(propertyAccessor, Objects::equals));
			return this;
		}
		
		public Set<AbstractDiff<?>> run(T t1, T t2) {
			Set<AbstractDiff<?>> result = new HashSet<>();
			propertiesToCompare.forEach(p -> {
				System.out.println("Comparing " + t1 + " vs " + t2 + " with " + p.getClass());
				// AccessorDefinition.toString(Accessors.accessor(propertyComparator.propertyAccessor))
				if (p instanceof ComparisonChain.CollectionComparisonChain) {
					Set<AbstractDiff<T>> differences = ((CollectionComparisonChain) p).collectionComparator.compare(t1, t2);
					result.addAll(differences.stream()
							.filter(d -> d.getState() != State.HELD).collect(Collectors.toList()));
					// going deeper if necessary
					ComparisonChain next = ((CollectionComparisonChain) p).next;
					if (next != null) {
						List<AbstractDiff<?>> collect = differences.stream()
								.filter(d -> d.getState() == State.HELD)
								.map(d -> next.run(d.getSourceInstance(), d.getReplacingInstance()))
								.flatMap(Set<AbstractDiff<?>>::stream)
								.collect(Collectors.toList());
						result.addAll(collect);
					}
				} else if (p instanceof ComparisonChain.PropertyComparator) {
					PropertyComparator<T, ?> propertyComparator = (PropertyComparator<T, ?>) p;
					PropertyDiff<?, ?> diff = propertyComparator.compare(t1, t2);
					System.out.println(t1 + " compared to " + t2 + " on "
							+ AccessorDefinition.toString(Accessors.accessor(propertyComparator.propertyAccessor))
							+ " : "
							+ propertyComparator.propertyAccessor.apply(t1)
							+ " vs "
							+ propertyComparator.propertyAccessor.apply(t2));
					if (diff != null) {
						result.add(diff);
					}
				}
			});
			return result;
		}
		
		private class CollectionComparisonChain<E, C extends Collection<E>> {
			
			private final CollectionComparator<T, E, C> collectionComparator;
			private final String string;
			private final String key;
			private ComparisonChain<?> next;
			
			private CollectionComparisonChain(SerializableFunction<T, C> collectionAccessor, SerializableFunction<E, ?> keyAccessor) {
				this.collectionComparator = new CollectionComparator<>(collectionAccessor, keyAccessor);
				key = AccessorDefinition.toString(Accessors.accessor(collectionAccessor));
				string = AccessorDefinition.toString(Accessors.accessor(keyAccessor));
			}
			
			public ComparisonChain<T> thenForeachHeld(ComparisonChain<?> next) {
				this.next = next;
				return ComparisonChain.this;
			}
			
			public Set<AbstractDiff<?>> run(T ddlElements1, T ddlElements2) {
				return ComparisonChain.this.run(ddlElements1, ddlElements2);
			}
		}
		
		static class CollectionComparator<T, E, C extends Collection<E>> {
			
			private final Function<T, C> collectionAccessor;
			private final Function<E, ?> keyAccessor;
			
			private CollectionComparator(Function<T, C> collectionAccessor, Function<E, ?> keyAccessor) {
				this.keyAccessor = keyAccessor;
				this.collectionAccessor = collectionAccessor;
			}
			
			Set<AbstractDiff<E>> compare(T t1, T t2) {
				CollectionDiffer<E, C, AbstractDiff<E>> collectionDiffer = null;
				C apply = collectionAccessor.apply(t1);
				if (apply instanceof Set) {
					collectionDiffer = (CollectionDiffer) new SetDiffer<>(keyAccessor);
				} else if (apply instanceof List) {
					collectionDiffer = (CollectionDiffer) new ListDiffer<>(keyAccessor);
				}
				C apply1 = collectionAccessor.apply(t2);
				return collectionDiffer.diff(apply, apply1);
			}
		}
		
		/**
		 * Comparator of a property between 2 objects of same type
		 *
		 * @param <T> object type
		 * @param <O> property value type
		 * @author Guillaume Mary
		 */
		static class PropertyComparator<T, O> {
			
			private final SerializableFunction<T, O> propertyAccessor;
			
			private final BiPredicate<O, O> predicate;
			private final String string;
			
			private PropertyComparator(SerializableFunction<T, O> propertyAccessor, BiPredicate<O, O> predicate) {
				this.propertyAccessor = propertyAccessor;
				this.predicate = predicate;
				string = AccessorDefinition.toString(Accessors.accessor(propertyAccessor));
			}
			
			PropertyDiff<T, O> compare(T t1, T t2) {
				O v1 = propertyAccessor.apply(t1);
				O v2 = propertyAccessor.apply(t2);
				boolean comparison = predicate.test(v1, v2);
				
				System.out.println(t1 + " vs " + t2 + " on "
						+ AccessorDefinition.toString(Accessors.accessor(propertyAccessor))
						+ " : "
						+ propertyAccessor.apply(t1)
						+ " vs "
						+ propertyAccessor.apply(t2)
				+ " (" + comparison + ")");
				
				if (!comparison) {
					return new PropertyDiff<>(propertyAccessor, t1, t2);
				}
				return null;
			}
			
			/**
			 * Storage of a property difference between 2 objects of same type
			 *
			 * @param <T> object type
			 * @param <O> property value type
			 * @author Guillaume Mary
			 */
			static class PropertyDiff<T, O> extends AbstractDiff<T> {
				
				private final SerializableFunction<T, O> propertyAccessor;
				
				/**
				 * Minimal constructor.
				 *
				 * @param sourceInstance initial instance
				 * @param replacingInstance replacing instance (may differ from source on attributes except id)
				 */
				public PropertyDiff(SerializableFunction<T, O> propertyAccessor, T sourceInstance, T replacingInstance) {
					super(State.HELD, sourceInstance, replacingInstance);
					this.propertyAccessor = propertyAccessor;
				}
				
				public SerializableFunction<T, ?> getPropertyAccessor() {
					return propertyAccessor;
				}
			}
		}
	}
}
