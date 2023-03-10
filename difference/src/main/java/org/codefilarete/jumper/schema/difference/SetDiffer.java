package org.codefilarete.jumper.schema.difference;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.jumper.schema.difference.State.*;

/**
 * A class to compute the differences between 2 collections of objects: addition, removal or held
 *
 * @param <T> bean type
 * @param <I> the type of the payload onto comparison will be done
 * @author Guillaume Mary
 */
public class SetDiffer<T, I> implements CollectionDiffer<T, Set<T>, Diff<T>> {
	
	private final Function<T, I> idProvider;
	
	public SetDiffer(Function<T, I> idProvider) {
		this.idProvider = idProvider;
	}
	
	/**
	 * Computes the differences between 2 sets. Comparison between objects will be done onto instance equals() method
	 *
	 * @param before the "source" Set
	 * @param after the modified Set
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	@Override
	public KeepOrderSet<Diff<T>> diff(Set<T> before, Set<T> after) {
		Map<I, T> beforeMappedOnIdentifier = Iterables.map(before, idProvider, Function.identity(), HashMap::new);
		Map<I, T> afterMappedOnIdentifier = Iterables.map(after, idProvider, Function.identity(), HashMap::new);
		
		KeepOrderSet<Diff<T>> result = new KeepOrderSet<>();
		
		for (Entry<I, T> entry : beforeMappedOnIdentifier.entrySet()) {
			T afterId = afterMappedOnIdentifier.get(entry.getKey());
			if (afterId != null) {
				result.add(new Diff<>(HELD, entry.getValue(), afterId));
			} else {
				result.add(new Diff<>(REMOVED, entry.getValue(), null));
			}
		}
		Map<I, T> addedElements = new HashMap<>(afterMappedOnIdentifier);
		addedElements.keySet().removeAll(beforeMappedOnIdentifier.keySet());
		for (Entry<I, T> identifiedEntry : addedElements.entrySet()) {
			result.add(new Diff<>(ADDED, null, identifiedEntry.getValue()));
		}
		return result;
	}
}
