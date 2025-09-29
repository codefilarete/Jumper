package org.codefilarete.jumper.schema.difference;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.tool.trace.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.jumper.schema.difference.State.*;
import static org.codefilarete.tool.collection.Arrays.asHashSet;
import static org.codefilarete.tool.collection.Arrays.asList;

class ListDifferTest {
	
	private static final Function<AbstractDiff<Country>, Long> SOURCE_ID_GETTER
			= Functions.link(AbstractDiff::getSourceInstance, Country::getId);
	private static final Function<AbstractDiff<Country>, Long> REPLACING_ID_GETTER
			= Functions.link(AbstractDiff::getReplacingInstance, Country::getId);
	
	private static final Comparator<AbstractDiff<Country>> STATE_THEN_ID_COMPARATOR = Comparator
			.<AbstractDiff<Country>, State>comparing(AbstractDiff::getState)
			.thenComparing(SOURCE_ID_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()))
			.thenComparing(REPLACING_ID_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()));
	
	private static class TestData {
		
		private final MutableInt longProvider = new MutableInt();
		private final Country country1 = new Country(longProvider.increment());
		private final Country country2 = new Country(longProvider.increment());
		private final Country country3 = new Country(longProvider.increment());
		private final Country country3Clone = new Country(country3.getId());
		private final Country country4 = new Country(longProvider.increment());
		private final Country country5 = new Country(longProvider.increment());
		
		private TestData() {
			country1.setName("France");
			country2.setName("Spain");
			country3.setName("Italy");
			country3Clone.setName(country3.getName() + " changed");
			country4.setName("England");
			country5.setName("Germany");
		}
	}
	
	static Object[][] diff_data() {
		TestData testData = new TestData();
		return new Object[][] {
				{
						asList(testData.country1, testData.country2, testData.country3),
						asList(testData.country2, testData.country1, testData.country3),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(1),
								new IndexedDiff<>(HELD, testData.country2, testData.country2)
										.addSourceIndex(1).addReplacerIndex(0),
								new IndexedDiff<>(HELD, testData.country3, testData.country3)
										.addSourceIndex(2).addReplacerIndex(2))
				},
				{
						asList(testData.country1),
						asList(testData.country1, testData.country2),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(0),
								new IndexedDiff<>(ADDED, null, testData.country2)
										.addReplacerIndex(1))
				},
				{
						asList(testData.country1, testData.country2),
						asList(testData.country1),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(0),
								new IndexedDiff<>(REMOVED, testData.country2, null)
										.addSourceIndex(1))
				},
				// with duplicates ...
				// ... one removed
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(0),
								new IndexedDiff<>(REMOVED, testData.country2, null)
										.addSourceIndex(1),
								new IndexedDiff<>(REMOVED, testData.country1, null)
										.addSourceIndex(2))
				},
				// ... none removed
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1, testData.country1),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addSourceIndex(2).addReplacerIndex(0).addReplacerIndex(1),
								new IndexedDiff<>(REMOVED, testData.country2, null)
										.addSourceIndex(1))
				},
				// ... all removed
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country2),
						asHashSet(new IndexedDiff<>(REMOVED, testData.country1, null)
										.addSourceIndex(0).addSourceIndex(2),
								new IndexedDiff<>(HELD, testData.country2, testData.country2)
										.addSourceIndex(1).addReplacerIndex(0))
				},
				// ... none removed but some added
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1, testData.country1, testData.country1, testData.country1),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addSourceIndex(2).addReplacerIndex(0).addReplacerIndex(1),
								new IndexedDiff<>(ADDED, null, testData.country1)
										.addReplacerIndex(2).addReplacerIndex(3),
								new IndexedDiff<>(REMOVED, testData.country2, null)
										.addSourceIndex(1))
				},
				// ... none removed but some added
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1, testData.country1, testData.country1, testData.country1, testData.country2, testData.country2),
						asHashSet(new IndexedDiff<>(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addSourceIndex(2).addReplacerIndex(0).addReplacerIndex(1),
								new IndexedDiff<>(ADDED, null, testData.country1)
										.addReplacerIndex(2).addReplacerIndex(3),
								new IndexedDiff<>(HELD, testData.country2, testData.country2)
										.addSourceIndex(1).addReplacerIndex(4),
								new IndexedDiff<>(ADDED, null, testData.country2)
										.addReplacerIndex(5))
				},
				// corner cases with empty sets
				{
						asList(),
						asList(testData.country1),
						asHashSet(new IndexedDiff<>(ADDED, null, testData.country1)
								.addReplacerIndex(0))
				},
				{
						asList(testData.country1),
						asList(),
						asHashSet(new IndexedDiff<>(REMOVED, testData.country1, null)
								.addSourceIndex(0))
				},
				{
						asList(),
						asList(),
						asHashSet()
				}
		};
	}
	
	@ParameterizedTest
	@MethodSource("diff_data")
	void diff(List<Country> set1, List<Country> set2, Set<IndexedDiff<Country>> expectedResult) {
		ListDiffer<Country, Long> testInstance = new ListDiffer<>(Country::getId);
		
		Set<IndexedDiff<Country>> diffs = testInstance.diff(set1, set2);
		
		// we use a comparator to ensure same order, then use a ToString method because assertThat(..) needs
		// an implementation of equals(..) and hashCode() on Diff which would have been made only for testing purpose
		TreeSet<IndexedDiff<Country>> treeSet1 = Arrays.asTreeSet(STATE_THEN_ID_COMPARATOR, diffs);
		TreeSet<IndexedDiff<Country>> treeSet2 = Arrays.asTreeSet(STATE_THEN_ID_COMPARATOR, expectedResult);
		
		assertThat(treeSet1)
				.usingElementComparator(Predicates.toComparator((diff1, diff2) ->
						diff1.getState() == diff2.getState()
						&& diff1.getSourceInstance() == diff2.getSourceInstance()
						&& diff1.getReplacingInstance() == diff2.getReplacingInstance()
						&& diff1.getSourceIndexes().equals(diff2.getSourceIndexes())
						&& diff1.getReplacerIndexes().equals(diff2.getReplacerIndexes())))
				.withRepresentation(new StandardRepresentation() {
					@Override
					protected String fallbackToStringOf(Object object) {
						return org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(object);
					}
				})
				.containsExactlyElementsOf(treeSet2);
	}
	
	@Test
	void diff_withClone_cloneMustBeReplacingInstance() {
		TestData testData = new TestData();
		Country country1Clone = new Country(testData.country1.getId());
		country1Clone.setName(testData.country1.getName());
		List<Country> set1 = asList(testData.country1, testData.country2, testData.country3);
		List<Country> set2 = asList(testData.country2, country1Clone, testData.country3);
		
		ListDiffer<Country, String> testInstance = new ListDiffer<>(Country::getName);	// we use a different predicate to prevent from using same comparator that equals(..) method
		Set<IndexedDiff<Country>> diffs = testInstance.diff(set1, set2);
		
		assertThat(diffs)
				.usingElementComparator((countryIndexedDiffComputed, countryIndexedDiffExpected) ->
						countryIndexedDiffComputed.getState().equals(countryIndexedDiffExpected.getState())
								// Comparing result on source and replacing instance because equals(
								// ..) is true on clone
								&& countryIndexedDiffComputed.getSourceInstance() == countryIndexedDiffExpected.getSourceInstance()
								&& countryIndexedDiffComputed.getReplacingInstance() == countryIndexedDiffExpected.getReplacingInstance()
								&& countryIndexedDiffComputed.getSourceIndexes().equals(countryIndexedDiffExpected.getSourceIndexes())
								&& countryIndexedDiffComputed.getReplacerIndexes().equals(countryIndexedDiffExpected.getReplacerIndexes()) ? 0 : -1
				)
				.containsExactlyInAnyOrder(
						new IndexedDiff<>(HELD, testData.country1, country1Clone, Arrays.asHashSet(0), Arrays.asHashSet(1)),
						new IndexedDiff<>(HELD, testData.country2, testData.country2, Arrays.asHashSet(1), Arrays.asHashSet(0)),
						new IndexedDiff<>(HELD, testData.country3, testData.country3, Arrays.asHashSet(2), Arrays.asHashSet(2)));
	}
}