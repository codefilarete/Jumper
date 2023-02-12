package org.codefilarete.jumper.ddl.engine;

import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.codefilarete.jumper.DialectResolver;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ServiceLoaderDialectResolver} that gets its registered {@link Dialect}s through JVM Service Provider and looks for the most compatible
 * one thanks to a compatibility algorithm.
 * <p>
 * This class will get available dialects and their compatibility as instances of {@link DialectResolverEntry}, themselves declared by JVM Service
 * Provider. Hence, it is expected that dialect implementors declare them through META-INF/services/DialectResolver.DialectResolverEntry
 * file. Then when {@link #determineDialect(DatabaseSignet)} is invoked, database metadata are compared to compatibility given by entries: only entries
 * whom product name exactly matches database one are kept, then comparing version, the highest dialect among smaller than database one is selected.
 * For example, if database is "A wonderful database 3.8", and 3 dialects for "A wonderful database" are present with "3.1", "3.5" and "4.0" versions,
 * then the "3.5" will be selected.
 * <p>
 * Why such algorithm ? because a dialect is expected to benefit from database features, hence its version should be close to the one of the
 * database that implements the feature, meaning at least equal but not lower : a "4.0" dialect may not be compatible with a "3.0" database. Therefore,
 * only smaller dialect versions are valuable, and among them, we take the closest one to benefit from best features. We also consider that databases
 * are retro-compatible so older dialects are still relevant.
 *
 * @author Guillaume Mary
 */
public class ServiceLoaderDialectResolver implements DialectResolver {
	
	private final Logger LOGGER = LoggerFactory.getLogger(ServiceLoaderDialectResolver.class);
	
	public Dialect determineDialect(DatabaseSignet databaseSignet) {
		ServiceLoader<DialectResolverEntry> dialects = ServiceLoader.load(DialectResolverEntry.class);
		return determineDialect(dialects, databaseSignet);
	}
	
	Dialect determineDialect(Iterable<? extends DialectResolverEntry> dialects, DatabaseSignet databaseSignet) {
		Nullable<DialectResolverEntry> matchingDialect = Nullable.nullable(giveMatchingEntry(dialects, databaseSignet));
		return matchingDialect.map(DialectResolver.DialectResolverEntry::getDialect)
				.getOr(() -> {
					LOGGER.warn("Default Dialect will be used because we were unable to determine a specific dialect to use for database \""
								+ databaseSignet.getProductName()
								+ " " + databaseSignet.getMajorVersion()
								+ "." + databaseSignet.getMinorVersion()
								+ "\" among " + Iterables.collectToList(dialects, o -> "{" + Strings.footPrint(o.getCompatibility(),
								DialectResolver.DatabaseSignet::toString) + "}"));
					return new Dialect();
				});
	}
	
	@VisibleForTesting
	@javax.annotation.Nullable
	DialectResolverEntry giveMatchingEntry(Iterable<? extends DialectResolverEntry> dialects, DatabaseSignet databaseSignet) {
		// only dialects that exactly matches database product name are kept
		Set<DialectResolverEntry> databaseDialects = Iterables.stream(dialects)
				.filter(entry -> entry.getCompatibility().getProductName().equals(databaseSignet.getProductName()))
				.collect(Collectors.toSet());
		
		if (databaseDialects.isEmpty()) {
			// no dialect for database, caller will handle that
			return null;
		} else {
			// sorting entries by compatibility versions to ease selection of the highest among the smaller than database version
			// Note: we could have used the stream way of collecting things, but it's a bit less readable
			TreeMap<DatabaseSignet, DialectResolverEntry> dialectPerSortedCompatibility = new TreeMap<>(DialectResolver.DatabaseSignet.COMPARATOR);
			databaseDialects.forEach(dialect -> dialectPerSortedCompatibility.merge(dialect.getCompatibility(), dialect, (c1, c2) -> {
				// we use same properties as DatabaseSignet comparator ones since we use a TreeMap based on it
				String printableSignet = Strings.footPrint(c1.getCompatibility(), DialectResolver.DatabaseSignet::toString);
				throw new IllegalStateException("Multiple dialects with same database compatibility found : " + printableSignet);
			}));
			
			// we select the highest dialect among the smaller than database version
			Entry<DatabaseSignet, DialectResolverEntry> foundEntry = dialectPerSortedCompatibility.floorEntry(databaseSignet);
			return foundEntry == null ? null : foundEntry.getValue();
		}
	}
	
}
