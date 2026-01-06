package org.codefilarete.jumper.schema.difference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.PairIterator.UntilBothIterator;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.jumper.schema.difference.State.*;

/**
 * A class to compute the differences between 2 collections of objects: addition, removal, or held
 *
 * @param <T> bean type
 * @author Guillaume Mary
 */
public class ListDiffer<T, I> implements CollectionDiffer<T, List<T>, IndexedDiff<T>> {
	
	private final Function<T, I> idProvider;
	private final BiPredicate<T, T> elementPredicate;
	private AccessorDefinition collectionAccessor;
	
	public ListDiffer(Function<T, I> idProvider) {
		this.idProvider = idProvider;
		this.elementPredicate = (t1, t2) -> Objects.equals(this.idProvider.apply(t1), this.idProvider.apply(t2));
	}
	
	public void setCollectionAccessor(AccessorDefinition collectionAccessor) {
		this.collectionAccessor = collectionAccessor;
	}
	
	/**
	 * Computes the differences between 2 lists. Comparison between objects will be done through given idProvider result
	 *
	 * @param before the "source" List
	 * @param after the modified List
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	@Override
	public KeepOrderSet<IndexedDiff<T>> diff(List<T> before, List<T> after) {
		// building Map of indexes per object
		Map<T, Set<Integer>> beforeIndexes = new HashMap<>();
		Map<I, Set<Integer>> beforeIndexesPerId = new HashMap<>();
		Map<T, Set<Integer>> afterIndexes = new HashMap<>();
		Map<I, Set<Integer>> afterIndexesPerId = new HashMap<>();
		MutableInt beforeIndex = new MutableInt(-1);	// because indexes should start at 0 as List does
		before.forEach(o -> {
			int index = beforeIndex.increment();
			beforeIndexes.computeIfAbsent(o, k -> new HashSet<>()).add(index);
			beforeIndexesPerId.computeIfAbsent(idProvider.apply(o), k -> new HashSet<>()).add(index);
		});
		MutableInt afterIndex = new MutableInt(-1);	// because indexes should start at 0 as List does
		after.forEach(o -> {
			int index = afterIndex.increment();
			afterIndexes.computeIfAbsent(o, k -> new HashSet<>()).add(index);
			afterIndexesPerId.computeIfAbsent(idProvider.apply(o), k -> new HashSet<>()).add(index);
		});
		
		KeepOrderSet<IndexedDiff<T>> result = new KeepOrderSet<>();
		
		// Removed instances are found with a simple minus
		Set<T> removeds = Iterables.minus(beforeIndexes.keySet(), afterIndexes.keySet(), elementPredicate);
		removeds.forEach(e -> result.add(new IndexedDiff<>(REMOVED, e, null, beforeIndexes.get(e), new HashSet<>())));
		
		// Added instances are found with a simple minus (reverse order of removed)
		Set<T> addeds = Iterables.minus(afterIndexes.keySet(), beforeIndexes.keySet(), elementPredicate);
		addeds.forEach(e -> result.add(new IndexedDiff<>(ADDED, null, e, new HashSet<>(), afterIndexes.get(e))));
		
		// There are several cases for "held" instances (those existing on both sides)
		// - if there are more instances in the new set, then those new are ADDED (with their new index)
		// - if there are fewer instances in the set, then the missing ones are REMOVED (with their old index)
		// - common instances are HELD (with their index)
		// This principle is applied with an Iterator of pairs of indexes : pairs contain before and after index.
		// - Pairs with a missing left or right value are declared added and removed, respectively
		// - Pairs with both values are declared held
		Set<T> helds = Iterables.intersect(afterIndexes.keySet(), beforeIndexes.keySet(), elementPredicate);
		helds.forEach(e -> {
			I id = idProvider.apply(e);
			Iterable<Duo<Integer, Integer>> indexPairs = () -> new UntilBothIterator<>(
					beforeIndexesPerId.get(id),
					afterIndexesPerId.get(id));
			IndexedDiff<T> removed = new IndexedDiff<>(REMOVED, e, null);
			IndexedDiff<T> held = new IndexedDiff<>(HELD,
													// Is this can be more efficient ? shouldn't we compute a Map of i vs before/after instead of iterating on before/after for each held ?
													// ... benchmark should be done
													Iterables.find(before, t -> Predicates.equalOrNull(idProvider.apply(t), id)),
													Iterables.find(after, t -> Predicates.equalOrNull(idProvider.apply(t), id)));
			IndexedDiff<T> added = new IndexedDiff<>(ADDED, null, e);
			for (Duo<? extends Integer, ? extends Integer> indexPair : indexPairs) {
				if (indexPair.getLeft() != null && indexPair.getRight() != null) {
					held.addSourceIndex(indexPair.getLeft());
					held.addReplacerIndex(indexPair.getRight());
				} else if (indexPair.getRight() == null) {
					removed.addSourceIndex(indexPair.getLeft());
				} else if (indexPair.getLeft() == null) {    // unnecessary "if" since this case is the obvious one
					added.addReplacerIndex(indexPair.getRight());
				}
			}
			// adding result of iteration to final result
			if (!removed.getSourceIndexes().isEmpty()) {
				result.add(removed);
			}
			if (!held.getReplacerIndexes().isEmpty()) {
				result.add(held);
			}
			if (!added.getReplacerIndexes().isEmpty()) {
				result.add(added);
			}
		});
		
		result.forEach(diff -> diff.setCollectionAccessor(collectionAccessor));
		
		return result;
	}
}
