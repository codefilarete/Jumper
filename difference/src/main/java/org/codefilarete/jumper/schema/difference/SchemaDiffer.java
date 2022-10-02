package org.codefilarete.jumper.schema.difference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.PrimaryKey;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableFunction;

public class SchemaDiffer {
	
	public static <I, O> BiPredicate<I, I> biPredicate(Function<I, O> mapper) {
		return (o1, o2) -> {
			if (o1 != null && o2 != null) {
				return Objects.equals(mapper.apply(o1), mapper.apply(o2));
			} else {
				return o1 == null && o2 == null;
			}
		};
	}
	
	private final ComparisonChain<Schema> comparisonChain;
	
	public SchemaDiffer() {
		this.comparisonChain = configure();
	}
	
	/**
	 * Implemented as such :
	 * - doesn't compare schema name
	 * - compares table presence by (strict) name
	 * - compares table columns by name, type, size, precision, nullity, auto-increment
	 * - compares index presence by (strict) name
	 * - compares index uniqueness and columns
	 *
	 * @return a {@link ComparisonChain} that can be completed or asked to {@link ComparisonChain#compare(Schema, Schema)} some schemas
	 */
	protected ComparisonChain<Schema> configure() {
		return comparisonChain(Schema.class)
				.compareOn(Schema::getTables, Table::getName, comparisonChain(Table.class)
						.compareOn(Table::getColumns, Column::getName, comparisonChain(Column.class)
								.compareOn(Column::getType)
								.compareOn(Column::getSize)
								.compareOn(Column::getPrecision)
								.compareOn(Column::isNullable)
								.compareOn(Column::isAutoIncrement))
						.compareOn(Table::getPrimaryKey, biPredicate(PrimaryKey::getName), comparisonChain(PrimaryKey.class)
								.compareOn(PrimaryKey::getColumns, Column::getName))
						)
				.compareOn(Schema::getIndexes, Index::getName, comparisonChain(Index.class)
						.compareOn(Index::isUnique)
						.compareOnMap(Index::getColumns, Column::getName)
				);
	}
	
	public Set<AbstractDiff<?>> compare(Schema schema1, Schema schema2) {
		return comparisonChain.run(schema1, schema2);
	}
	
	public <T> ComparisonChain<T> comparisonChain(Class<T> clazz) {
		return new ComparisonChain<>(clazz);
	}
	
	public static class ComparisonChain<T> {
		
		private final List<Object> propertiesToCompare = new ArrayList<>();
		
		public ComparisonChain(Class<T> ignored) {
		}
		
		public <E, C extends Collection<E>> ComparisonChain<T> compareOn(SerializableFunction<T, C> collectionAccessor, SerializableFunction<E, ?> keyAccessor) {
			return compareOn(collectionAccessor, keyAccessor, null);
		}
		
		public <E, C extends Collection<E>> ComparisonChain<T> compareOn(SerializableFunction<T, C> collectionAccessor, SerializableFunction<E, ?> keyAccessor, ComparisonChain<E> deeperComparison) {
			CollectionComparator<T, E, C> collectionComparison = new CollectionComparator<>(collectionAccessor, keyAccessor);
			this.propertiesToCompare.add(collectionComparison);
			collectionComparison.next = deeperComparison;
			return this;
		}
		
		public <K, V, M extends Map<K, V>> ComparisonChain<T> compareOnMap(SerializableFunction<T, M> collectionAccessor, SerializableFunction<K, ?> keyAccessor) {
			return compareOnMap(collectionAccessor, keyAccessor, null);
		}
		
		public <K, V, M extends Map<K, V>> ComparisonChain<T> compareOnMap(SerializableFunction<T, M> collectionAccessor, SerializableFunction<K, ?> keyAccessor, ComparisonChain<Map.Entry<K, V>> deeperComparison) {
			MapComparator<T, K, V, M> collectionComparison = new MapComparator<>(collectionAccessor, keyAccessor);
			this.propertiesToCompare.add(collectionComparison);
			collectionComparison.next = deeperComparison;
			return this;
		}
		
		public <O> ComparisonChain<T> compareOn(SerializableFunction<T, O> propertyAccessor) {
			this.propertiesToCompare.add(new PropertyComparator<>(propertyAccessor, Objects::equals));
			return this;
		}
		
		public <O> ComparisonChain<T> compareOn(SerializableFunction<T, O> propertyAccessor, BiPredicate<O, O> predicate, ComparisonChain<O> deeperComparison) {
			PropertyComparator<T, O> propertyComparator = new PropertyComparator<>(propertyAccessor, predicate);
			this.propertiesToCompare.add(propertyComparator);
			propertyComparator.next = deeperComparison;
			return this;
		}
		
		public Set<AbstractDiff<?>> run(T t1, T t2) {
			if (t1 == null && t2 == null) {
				return Collections.emptySet();
			} else if (t1 == null) {
				return Arrays.asHashSet(new Diff<>(State.ADDED, null, t2));
			} else if (t2 == null) {
				return Arrays.asHashSet(new Diff<>(State.REMOVED, t1, null));
			} else {
				Set<AbstractDiff<?>> result = new HashSet<>();
				propertiesToCompare.forEach(p -> {
					if (p instanceof ComparisonChain.CollectionComparator) {
						result.addAll(((CollectionComparator) p).compare(t1, t2));
					} else if (p instanceof ComparisonChain.MapComparator) {
						result.addAll(((MapComparator) p).compare(t1, t2));
					} else if (p instanceof ComparisonChain.PropertyComparator) {
						PropertyComparator<T, ?> propertyComparator = (PropertyComparator<T, ?>) p;
						Set<AbstractDiff<?>> diffs = propertyComparator.compare(t1, t2);
						if (diffs != null) {
							result.addAll(diffs);
						}
					}
				});
				return result;
			}
		}
		
		static class MapComparator<T, K, V, M extends Map<K, V>> {
			
			private final SerializableFunction<T, M> mapAccessor;
			private final SerializableFunction<K, ?> keyAccessor;
			private ComparisonChain<Map.Entry<K, V>> next;
			
			MapComparator(SerializableFunction<T, M> mapAccessor, SerializableFunction<K, ?> keyAccessor) {
				this.mapAccessor = mapAccessor;
				this.keyAccessor = keyAccessor;
			}
			
			Set<AbstractDiff<?>> compare(T t1, T t2) {
				Set<AbstractDiff<?>> result = new KeepOrderSet<>();
				MapDiffer<K, V, ?> collectionDiffer = new MapDiffer<>(keyAccessor);
				KeepOrderSet<Diff<Entry<K, V>>> mapPresences = collectionDiffer.diff(this.mapAccessor.apply(t1), this.mapAccessor.apply(t2));
				
				result.addAll(mapPresences.stream()
						.filter(d -> d.getState() != State.HELD).collect(Collectors.toList()));
				if (next != null) {
					List<AbstractDiff<?>> collect = mapPresences.stream()
							.filter(d -> d.getState() == State.HELD)
							.map(d -> next.run(d.getSourceInstance(), d.getReplacingInstance()))
							.flatMap(Set<AbstractDiff<?>>::stream)
							.collect(Collectors.toList());
					result.addAll(collect);
				}
				return result;
			}
		}
		
		static class CollectionComparator<T, E, C extends Collection<E>> {
			
			private final SerializableFunction<T, C> collectionAccessor;
			private final SerializableFunction<E, ?> keyAccessor;
			private ComparisonChain<E> next;
			
			private CollectionComparator(SerializableFunction<T, C> collectionAccessor, SerializableFunction<E, ?> keyAccessor) {
				this.keyAccessor = keyAccessor;
				this.collectionAccessor = collectionAccessor;
			}
			
			Set<AbstractDiff<?>> compare(T t1, T t2) {
				Set<AbstractDiff<?>> result = new KeepOrderSet<>();
				CollectionDiffer<E, C, AbstractDiff<E>> collectionDiffer = null;
				C apply = collectionAccessor.apply(t1);
				if (apply instanceof Set) {
					collectionDiffer = (CollectionDiffer) new SetDiffer<>(keyAccessor);
				} else if (apply instanceof List) {
					collectionDiffer = (CollectionDiffer) new ListDiffer<>(keyAccessor);
				}
				C apply1 = collectionAccessor.apply(t2);
				KeepOrderSet<AbstractDiff<E>> collectionPresences = collectionDiffer.diff(apply, apply1);
				
				result.addAll(collectionPresences.stream()
						.filter(d -> d.getState() != State.HELD).collect(Collectors.toList()));
				if (next != null) {
					List<AbstractDiff<?>> collect = collectionPresences.stream()
							.filter(d -> d.getState() == State.HELD)
							.map(d -> next.run(d.getSourceInstance(), d.getReplacingInstance()))
							.flatMap(Set<AbstractDiff<?>>::stream)
							.collect(Collectors.toList());
					result.addAll(collect);
				}
				
				return result;
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
			public ComparisonChain<O> next;
			
			private PropertyComparator(SerializableFunction<T, O> propertyAccessor, BiPredicate<O, O> predicate) {
				this.propertyAccessor = propertyAccessor;
				this.predicate = predicate;
			}
			
			Set<AbstractDiff<?>> compare(T t1, T t2) {
				O v1 = propertyAccessor.apply(t1);
				O v2 = propertyAccessor.apply(t2);
				boolean comparison = predicate.test(v1, v2);
				if (!comparison) {
					return Arrays.asHashSet(new PropertyDiff<>(propertyAccessor, t1, t2));
				} else if (next != null) {
					return next.run(v1, v2);
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
