package org.codefilarete.jumper.schema.difference;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.tool.trace.MutableInt;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.jumper.schema.difference.State.*;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Arrays.asHashSet;

class MapDifferTest {
	
	private static Map<Country, String> asMap(Country... elements) {
		return Stream.of(elements).collect(Collectors.toMap(Function.identity(), Country::getName));
	}
	
	private static Map.Entry<Country, String> asEntry(Country element) {
		return new SimpleEntry<>(element, element.getName());
	}
	
	private static class TestData {
		
		private final MutableInt longProvider = new MutableInt();
		private final Country country1 = new Country(longProvider.increment());
		private final Country country2 = new Country(longProvider.increment());
		private final Country country3 = new Country(longProvider.increment());
		
		private TestData() {
			country1.setName("France");
			country2.setName("Spain");
			country3.setName("Italy");
		}
	}
	
	static Object[][] diff_data() {
		TestData testData = new TestData();
		return new Object[][] {
				{
						asMap(testData.country1, testData.country2, testData.country3),
						asMap(testData.country2, testData.country1, testData.country3),
						asHashSet(new Diff<>(HELD, asEntry(testData.country1), asEntry(testData.country1)),
								new Diff<>(HELD, asEntry(testData.country2), asEntry(testData.country2)),
								new Diff<>(HELD, asEntry(testData.country3), asEntry(testData.country3)))
				},
				{
						asMap(testData.country1),
						asMap(testData.country1, testData.country2),
						asHashSet(new Diff<>(HELD, asEntry(testData.country1), asEntry(testData.country1)),
								new Diff<>(ADDED, null, asEntry(testData.country2)))
				},
				{
						asMap(testData.country1, testData.country2),
						asMap(testData.country1),
						asHashSet(new Diff<>(HELD, asEntry(testData.country1), asEntry(testData.country1)),
								new Diff<>(REMOVED, asEntry(testData.country2), null))
				},
				// corner cases with empty sets
				{
						asMap(),
						asMap(testData.country1),
						asHashSet(new Diff<>(ADDED, null, asEntry(testData.country1)))
				},
				{
						asMap(testData.country1),
						asMap(),
						asHashSet(new Diff<>(REMOVED, asEntry(testData.country1), null))
				},
				{
						asMap(),
						asMap(),
						asHashSet()
				}
		};
	}
	
	@ParameterizedTest
	@MethodSource("diff_data")
	void diff(Map<Country, String> set1, Map<Country, String> set2, Set<Diff<Entry<Country, String>>> expectedResult) {
		MapDiffer<Country, String, Long> testInstance = new MapDiffer<>(Country::getId);
		
		KeepOrderSet<Diff<Entry<Country, String>>> diffs = testInstance.diff(set1, set2);
		
		assertThat(diffs)
				.usingElementComparator(Predicates.toComparator((diff1, diff2) ->
						diff1.getState() == diff2.getState()
								&& nullable(diff1.getSourceInstance()).map(Entry::getKey).get() == nullable(diff2.getSourceInstance()).map(Entry::getKey).get()
								&& Objects.equals(nullable(diff1.getSourceInstance()).map(Entry::getValue).get(), nullable(diff2.getSourceInstance()).map(Entry::getValue).get())
								&& nullable(diff1.getReplacingInstance()).map(Entry::getKey).get() == nullable(diff2.getReplacingInstance()).map(Entry::getKey).get()
								&& Objects.equals(nullable(diff1.getReplacingInstance()).map(Entry::getValue).get(), nullable(diff2.getReplacingInstance()).map(Entry::getValue).get())
				))
				.withRepresentation(new StandardRepresentation() {
					@Override
					protected String fallbackToStringOf(Object object) {
						return org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(object);
					}
				})
				.containsAnyElementsOf(expectedResult);
	}
	
}