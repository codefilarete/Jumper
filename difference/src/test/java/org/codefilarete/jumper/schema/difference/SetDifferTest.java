package org.codefilarete.jumper.schema.difference;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.jumper.schema.difference.State.*;
import static org.codefilarete.tool.collection.Arrays.asHashSet;

class SetDifferTest {
	
	private static final Function<AbstractDiff<Country>, Long> SOURCE_ID_GETTER
			= Functions.link(AbstractDiff::getSourceInstance, Country::getId);
	private static final Function<AbstractDiff<Country>, Long> REPLACING_ID_GETTER
			= Functions.link(AbstractDiff::getReplacingInstance, Country::getId);
	
	private static final Comparator<AbstractDiff<Country>> STATE_THEN_ID_COMPARATOR = Comparator
			.<AbstractDiff<Country>, State>comparing(AbstractDiff::getState)
			.thenComparing(SOURCE_ID_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()))
			.thenComparing(REPLACING_ID_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()));
	
	private static class TestData {
		
		private final ModifiableInt longProvider = new ModifiableInt();
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
						asHashSet(testData.country1, testData.country2, testData.country3),
						asHashSet(testData.country3Clone, testData.country4, testData.country5),
						Arrays.asHashSet(new Diff<>(ADDED, null, testData.country4),
								new Diff<>(ADDED, null, testData.country5),
								new Diff<>(REMOVED, testData.country1, null),
								new Diff<>(REMOVED, testData.country2, null),
								new Diff<>(HELD, testData.country3, testData.country3Clone))
				},
				// corner cases with empty sets
				{
						asHashSet(),
						asHashSet(testData.country1),
						asHashSet(new Diff<>(ADDED, null, testData.country1))
				},
				{
						asHashSet(testData.country1),
						asHashSet(),
						asHashSet(new Diff<>(REMOVED, testData.country1, null))
				},
				{
						asHashSet(),
						asHashSet(),
						asHashSet()
				}
		};
	}
	
	@ParameterizedTest
	@MethodSource("diff_data")
	void diff(Set<Country> set1, Set<Country> set2, Set<Diff<Country>> expectedResult) {
		SetDiffer<Country, Long> testInstance = new SetDiffer<>(Country::getId);
		
		Set<Diff<Country>> diffs = testInstance.diff(set1, set2);
		
		// we use a comparator to ensure same order, then use a ToString method because assertThat(..) needs
		// an implementation of equals(..) and hashCode() on Diff which would have been made only for testing purpose
		// we use a comparator to ensure same order, then use a ToString method because assertThat(..) needs
		// an implementation of equals(..) and hashCode() on Diff which would have been made only for testing purpose
		TreeSet<Diff<Country>> treeSet1 = Arrays.asTreeSet(STATE_THEN_ID_COMPARATOR, diffs);
		TreeSet<Diff<Country>> treeSet2 = Arrays.asTreeSet(STATE_THEN_ID_COMPARATOR, expectedResult);
		
		assertThat(treeSet1)
				.usingElementComparator(Predicates.toComparator((diff1, diff2) ->
						diff1.getState() == diff2.getState()
								&& diff1.getSourceInstance() == diff2.getSourceInstance()
								&& diff1.getReplacingInstance() == diff2.getReplacingInstance()))
				.withRepresentation(new StandardRepresentation() {
					@Override
					protected String fallbackToStringOf(Object object) {
						return org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(object);
					}
				})
				.containsExactlyElementsOf(treeSet2);
	}
	
}