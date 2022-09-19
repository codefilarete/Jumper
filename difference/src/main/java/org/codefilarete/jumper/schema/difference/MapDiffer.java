package org.codefilarete.jumper.schema.difference;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A class to compute the differences between 2 {@link Map}s : key addition, key removal, difference on value on held key
 *
 * Note that it can't inherit fom {@link CollectionDiffer} because {@link Map} doesn't extend {@link java.util.Collection}
 * nor {@link Iterable}
 *
 * @param <K> {@link Map} key type
 * @param <V> {@link Map} value type
 * @param <I> key property type to compare keys on
 * @author Guillaume Mary
 */
public class MapDiffer<K, V, I> {
	
	private final Function<K, I> idProvider;
	private final SetDiffer<Entry<K, V>, I> entriesDiffer;
	
	public MapDiffer(Function<K, I> idProvider) {
		this.idProvider = idProvider;
		this.entriesDiffer = new SetDiffer<>(e -> this.idProvider.apply(e.getKey()));
	}
	
	public KeepOrderSet<Diff<Map.Entry<K, V>>> diff(Map<K, V> before, Map<K, V> after) {
		return entriesDiffer.diff(before.entrySet(), after.entrySet());
	}
}
