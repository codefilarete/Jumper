package org.codefilarete.jumper.schema.difference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Indexable;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.ForeignKey;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.PrimaryKey;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.UniqueConstraint;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.View;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.View.PseudoColumn;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.SchemaElement;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.TableElement;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator.PropertyDiff;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableFunction;

public class SchemaDiffer {
	
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
								.compareOn(Column::getScale)
								.compareOn(Column::isNullable)
								.compareOn(Column::isAutoIncrement))
						.compareOn(Table::getPrimaryKey, comparisonChain(PrimaryKey.class)
								.compareOn(PrimaryKey::getName)
								.compareOn(PrimaryKey::getColumns, Column::getName))
						)
				.compareOn(Schema::getIndexes, Index::getName, comparisonChain(Index.class)
						.compareOn(Index::isUnique)
						.compareOnMap(Index::getColumns, Indexable::getName))
				.compareOn(Schema::getViews, View::getName, comparisonChain(View.class)
						.compareOn(View::getColumns, PseudoColumn::getName, comparisonChain(PseudoColumn.class)
								.compareOn(PseudoColumn::getType)
								.compareOn(PseudoColumn::getSize)
								.compareOn(PseudoColumn::getPrecision)
								.compareOn(PseudoColumn::isNullable)))
				.compareOn(schema -> schema.getTables().stream().flatMap(t -> t.getForeignKeys().stream()).collect(Collectors.toSet()),
						"Foreign keys",
						// We uses the foreign keys columns as a key of identifier because we consider them more important than the foreign key name
						fk -> fk.getColumns().stream().map(Column::getName).collect(Collectors.joining(", ")),
						comparisonChain(ForeignKey.class)
						.compareOn(ForeignKey::getColumns, Column::getName)
						.compareOn(ForeignKey::getTargetColumns, Column::getName)
						.compareOn(ForeignKey::getName))
				.compareOn(schema -> schema.getTables().stream().flatMap(t -> t.getUniqueConstraints().stream()).collect(Collectors.toSet()),
						"Unique Constraints",
						uk -> uk.getColumns().stream().map(Indexable::getName).collect(Collectors.joining(", ")),
						comparisonChain(UniqueConstraint.class)
								.compareOn(UniqueConstraint::getColumns, Indexable::getName)
								.compareOn(UniqueConstraint::getName)
				);
	}
	
	public Set<AbstractDiff<?>> compare(Schema schema1, Schema schema2) {
		return comparisonChain.run(schema1, schema2);
	}
	
	public void compareAndPrint(Schema schema1, Schema schema2) {
		Set<AbstractDiff<?>> diffs = comparisonChain.run(schema1, schema2);
		
		System.out.println("Added elements in " + schema2.getName() + " but missing in " + schema1.getName());
		
		Map<? extends Class<?>, List<AbstractDiff<?>>> addedPerType = diffs.stream()
				.filter(d -> d.getState() == State.ADDED)
				.collect(Collectors.groupingBy(diff -> diff.getReplacingInstance().getClass()));
		
		addedPerType.forEach((key, value) -> {
			System.out.println(key.getSimpleName());
			Comparator<AbstractDiff<SchemaElement>> comparing = Comparator
					.comparing((AbstractDiff<SchemaElement> diff) -> diff.getReplacingInstance().getSchema().getName())
					.thenComparing((AbstractDiff<SchemaElement> d) -> {
						if (d instanceof TableElement) {
							return ((TableElement) d.getReplacingInstance()).getTable().getName();
						} else {
							return d.getReplacingInstance().toString();
						}
					});
			value.stream().map(o -> (AbstractDiff<SchemaElement>) o)
					.sorted(comparing)
					.forEach(d -> {
						System.out.println("\t" + d.getReplacingInstance());
					});
		});
		
		System.out.println("Modified elements between " + schema2.getName() + " and " + schema1.getName());
		Map<? extends Class<?>, List<AbstractDiff<?>>> heldPerType = diffs.stream()
				.filter(d -> d.getState() == State.HELD)
				.collect(Collectors.groupingBy(diff -> diff.getReplacingInstance().getClass()));
		
		heldPerType.forEach((key, value) -> {
			System.out.println(key.getSimpleName());
			Comparator<AbstractDiff<SchemaElement>> comparing = Comparator
					.comparing((AbstractDiff<SchemaElement> diff) -> diff.getReplacingInstance().getSchema().getName())
					.thenComparing((AbstractDiff<SchemaElement> d) -> {
						if (d instanceof TableElement) {
							return ((TableElement) d.getReplacingInstance()).getTable().getName();
						} else {
							return d.getReplacingInstance().toString();
						}
					});
			value.stream().map(o -> (AbstractDiff<SchemaElement>) o)
					.sorted(comparing)
					.forEach(d -> {
						if (d instanceof PropertyComparator.PropertyDiff) {
							String propertyName = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(((PropertyDiff<?, ?>) d).getPropertyAccessor())).getName();
							System.out.println("\t" + propertyName + ": " + d.getReplacingInstance() + " vs " + d.getSourceInstance());
						} else {
							System.out.println("\t" + d.getReplacingInstance());
						}
					});
		});
		
		System.out.println("Missing elements in " + schema2.getName() + " but added in " + schema1.getName());
		Map<? extends Class<?>, List<AbstractDiff<?>>> removedPerType = diffs.stream()
				.filter(d -> d.getState() == State.REMOVED)
				.collect(Collectors.groupingBy(diff -> diff.getSourceInstance().getClass()));
		
		removedPerType.forEach((key, value) -> {
			System.out.println(key.getSimpleName());
			Comparator<AbstractDiff<SchemaElement>> comparing = Comparator
					.comparing((AbstractDiff<SchemaElement> diff) -> diff.getSourceInstance().getSchema().getName())
					.thenComparing((AbstractDiff<SchemaElement> d) -> {
						if (d instanceof TableElement) {
							return ((TableElement) d.getSourceInstance()).getTable().getName();
						} else {
							return d.getSourceInstance().toString();
						}
					});
			value.stream().map(o -> (AbstractDiff<SchemaElement>) o)
					.sorted(comparing)
					.forEach(d -> {
						System.out.println("\t" + d.getSourceInstance());
					});
		});
	}
	
	protected <T> ComparisonChain<T> comparisonChain(Class<T> clazz) {
		return new ComparisonChain<>(clazz);
	}
	
	public static class ComparisonChain<T> {
		
		private final Class<T> comparedType;
		
		private final List<Object> propertiesToCompare = new ArrayList<>();
		
		public ComparisonChain(Class<T> comparedType) {
			this.comparedType = comparedType;
		}
		
		public Class<T> getComparedType() {
			return comparedType;
		}
		
		public <E, C extends Collection<E>> ComparisonChain<T> compareOn(SerializableFunction<T, C> collectionAccessor, Function<E, ?> keyAccessor) {
			return compareOn(collectionAccessor, keyAccessor, null);
		}
		
		public <E, C extends Collection<E>> ComparisonChain<T> compareOn(Function<T, C> collectionAccessor, String collectionAccessorDescription, Function<E, ?> keyAccessor, ComparisonChain<E> deeperComparison) {
			CollectionComparator<T, E, C> collectionComparison = new CollectionComparator<>(collectionAccessor, keyAccessor);
			this.propertiesToCompare.add(collectionComparison);
			collectionComparison.next = deeperComparison;
			return this;
		}
		
		public <E, C extends Collection<E>> ComparisonChain<T> compareOn(SerializableFunction<T, C> collectionAccessor, Function<E, ?> keyAccessor, ComparisonChain<E> deeperComparison) {
			CollectionComparator<T, E, C> collectionComparison = new CollectionComparator<>(collectionAccessor, keyAccessor);
			this.propertiesToCompare.add(collectionComparison);
			collectionComparison.next = deeperComparison;
			return this;
		}
		
		public <K, V, M extends Map<K, V>> ComparisonChain<T> compareOnMap(SerializableFunction<T, M> collectionAccessor, Function<K, ?> keyAccessor) {
			return compareOnMap(collectionAccessor, keyAccessor, null);
		}
		
		public <K, V, M extends Map<K, V>> ComparisonChain<T> compareOnMap(SerializableFunction<T, M> collectionAccessor, Function<K, ?> keyAccessor, ComparisonChain<Entry<K, V>> deeperComparison) {
			MapComparator<T, K, V, M> collectionComparison = new MapComparator<>(collectionAccessor, keyAccessor);
			this.propertiesToCompare.add(collectionComparison);
			collectionComparison.next = deeperComparison;
			return this;
		}
		
		public <O> ComparisonChain<T> compareOn(SerializableFunction<T, O> propertyAccessor) {
			this.propertiesToCompare.add(new PropertyComparator<>(propertyAccessor, Objects::equals));
			return this;
		}
		
		public <O> ComparisonChain<T> compareOn(SerializableFunction<T, O> propertyAccessor, ComparisonChain<O> deeperComparison) {
			PropertyComparator<T, O> propertyComparator = new PropertyComparator<>(propertyAccessor);
			this.propertiesToCompare.add(propertyComparator);
			propertyComparator.deepComparator = deeperComparison;
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
			
			private final Function<T, M> mapAccessor;
			private final Function<K, ?> keyAccessor;
			private ComparisonChain<Entry<K, V>> next;
			
			MapComparator(Function<T, M> mapAccessor, Function<K, ?> keyAccessor) {
				this.mapAccessor = mapAccessor;
				this.keyAccessor = keyAccessor;
			}
			
			Set<AbstractDiff<?>> compare(T t1, T t2) {
				Set<AbstractDiff<?>> result = new KeepOrderSet<>();
				MapDiffer<K, V, ?> mapDiffer = new MapDiffer<>(keyAccessor);
				KeepOrderSet<Diff<Entry<K, V>>> mapPresences = mapDiffer.diff(this.mapAccessor.apply(t1), this.mapAccessor.apply(t2));
				
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
			
			private final Function<T, C> collectionAccessor;
			private final Function<E, ?> keyAccessor;
			private ComparisonChain<E> next;
			
			private CollectionComparator(Function<T, C> collectionAccessor, Function<E, ?> keyAccessor) {
				this.collectionAccessor = collectionAccessor;
				this.keyAccessor = keyAccessor;
			}
			
			Set<AbstractDiff<?>> compare(T t1, T t2) {
				Set<AbstractDiff<?>> result = new KeepOrderSet<>();
				C collection1 = collectionAccessor.apply(t1);
				AccessorDefinition collectionAccessorDefinition = null;
				if (collectionAccessor instanceof SerializableFunction) {
					PropertyAccessor<T, C> accessor = Accessors.accessor((SerializableFunction<T, C>) collectionAccessor);
					collectionAccessorDefinition = AccessorDefinition.giveDefinition(accessor);
				}
				C collection2 = collectionAccessor.apply(t2);
				
				CollectionDiffer<E, Collection<E>, AbstractDiff<E>> collectionDiffer = null;
				Collection<E> collectionToCompare1 = collection1;
				Collection<E> collectionToCompare2 = collection2;
				if (collection1 instanceof LinkedHashSet || collection1 instanceof TreeSet || collection1 instanceof KeepOrderSet) {
					// to respect the order of the Set, we uses a ListDiffer (that keep track of indexes), hence we have to transform the Sets to Lists
					collectionToCompare1 = new ArrayList<>(collection1);
					collectionToCompare2 = new ArrayList<>(collection2);
					ListDiffer<E, ?> setDiffer = new ListDiffer<>(keyAccessor);
					setDiffer.setCollectionAccessor(collectionAccessorDefinition);
					collectionDiffer = (CollectionDiffer) setDiffer;
				} else if (collection1 instanceof Set) {
					SetDiffer<E, ?> setDiffer = new SetDiffer<>(keyAccessor);
					setDiffer.setCollectionAccessor(collectionAccessorDefinition);
					collectionDiffer = (CollectionDiffer) setDiffer;
				} else if (collection1 instanceof List) {
					ListDiffer<E, ?> listDiffer = new ListDiffer<>(keyAccessor);
					listDiffer.setCollectionAccessor(collectionAccessorDefinition);
					collectionDiffer = (CollectionDiffer) listDiffer;
				} else if (collection1 instanceof Queue) {
					QueueDiffer<E, ?> queueDiffer = new QueueDiffer<>(keyAccessor);
					queueDiffer.setCollectionAccessor(collectionAccessorDefinition);
					collectionDiffer = (CollectionDiffer) queueDiffer;
				}
				KeepOrderSet<AbstractDiff<E>> collectionPresences = collectionDiffer.diff(collectionToCompare1, collectionToCompare2);
				
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
		public static class PropertyComparator<T, O> {
			
			private final SerializableFunction<T, O> propertyAccessor;
			
			private final BiPredicate<O, O> propertyPredicate;
			public ComparisonChain<O> deepComparator;
			
			private PropertyComparator(SerializableFunction<T, O> propertyAccessor) {
				// Predicate explanation : if property is null on both instances then they are considered equal, and
				// if both properties are not null, then they are considered equal too because we'll chain with
				// deepComparator, hence we don't considered them equal only if one of them is null and the other is not.
				this(propertyAccessor, (o1, o2) -> (o1 != null) == (o2 != null));
			}
			
			private PropertyComparator(SerializableFunction<T, O> propertyAccessor, BiPredicate<O, O> propertyPredicate) {
				this.propertyAccessor = propertyAccessor;
				this.propertyPredicate = propertyPredicate;
			}
			
			Set<AbstractDiff<?>> compare(T t1, T t2) {
				O v1 = propertyAccessor.apply(t1);
				O v2 = propertyAccessor.apply(t2);
				boolean comparison = propertyPredicate.test(v1, v2);
				if (!comparison) {
					return Arrays.asHashSet(new PropertyDiff<>(propertyAccessor, t1, t2));
				} else if (deepComparator != null) {
					return deepComparator.run(v1, v2);
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
			public static class PropertyDiff<T, O> extends AbstractDiff<T> {
				
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
