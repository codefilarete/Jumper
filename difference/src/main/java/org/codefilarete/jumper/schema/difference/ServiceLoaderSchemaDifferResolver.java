package org.codefilarete.jumper.schema.difference;

import java.sql.Connection;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.codefilarete.jumper.DialectResolver.DatabaseSignet;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;

/**
 * Implementation of {@link SchemaDifferResolver} that gets its registered {@link SchemaDiffer}s through JVM Service Provider and looks for the most compatible
 * one thanks to a compatibility algorithm.
 * 
 * This class will get available SchemaDiffers and their compatibility as instances of {@link SchemaDifferResolverEntry}, themselves declared by JVM Service
 * Provider. Hence, it is expected that SchemaDiffer implementors declare them through META-INF/services/SchemaDifferResolver.SchemaDifferResolverEntry
 * file. Then when {@link #determineSchemaDifference(Connection)} is invoked, database metadata are compared to compatibility given by entries: only entries
 * whom product name exactly matches database one are kept, then comparing version, the highest SchemaDiffer among smaller than database one is selected.
 * For example, if database is "A wonderful database 3.8", and 3 SchemaDiffers for "A wonderful database" are present with "3.1", "3.5" and "4.0" versions,
 * then the "3.5" will be selected.
 * 
 * Why such algorithm ? because a SchemaDiffer is expected to benefit from database features, hence its version should be close to the one of the
 * database that implements the feature, meaning at least equal but not lower : a "4.0" SchemaDiffer may not be compatible with a "3.0" database. Therefore,
 * only smaller SchemaDiffer versions are valuable, and among them, we take the closest one to benefit from best features. We also consider that databases
 * are retro-compatible so older SchemaDiffers are still relevant. 
 * 
 * @author Guillaume Mary
 */
public class ServiceLoaderSchemaDifferResolver implements SchemaDifferResolver {
	
	@Override
	public SchemaDiffer determineSchemaDifference(Connection conn) {
		DatabaseSignet databaseSignet = DatabaseSignet.fromMetadata(conn);
		ServiceLoader<SchemaDifferResolverEntry> SchemaDiffers = ServiceLoader.load(SchemaDifferResolverEntry.class);
		return determineSchemaDiffer(SchemaDiffers, databaseSignet);
	}
	
	SchemaDiffer determineSchemaDiffer(Iterable<? extends SchemaDifferResolverEntry> SchemaDiffers, DatabaseSignet databaseSignet) {
		Nullable<SchemaDifferResolverEntry> matchingSchemaDiffer = Nullable.nullable(giveMatchingEntry(SchemaDiffers, databaseSignet));
		return matchingSchemaDiffer.map(SchemaDifferResolverEntry::getSchemaDiffer).getOrThrow(
				() -> new IllegalStateException(
						"Unable to determine SchemaDiffer to use for database \""
								+ databaseSignet.getProductName()
								+ " " + databaseSignet.getMajorVersion()
								+ "." + databaseSignet.getMinorVersion()
								+ "\" among " + Iterables.collectToList(SchemaDiffers, o -> "{" + Strings.footPrint(o.getCompatibility(),
																DatabaseSignet::toString) + "}")));
	}
	
	@VisibleForTesting
	@javax.annotation.Nullable
	SchemaDifferResolverEntry giveMatchingEntry(Iterable<? extends SchemaDifferResolverEntry> SchemaDiffers, DatabaseSignet databaseSignet) {
		// only SchemaDiffers that exactly matches database product name are kept
		Set<SchemaDifferResolverEntry> databaseSchemaDiffers = Iterables.stream(SchemaDiffers)
				.filter(entry -> entry.getCompatibility().getProductName().equals(databaseSignet.getProductName()))
				.collect(Collectors.toSet());
		
		if (databaseSchemaDiffers.isEmpty()) {
			// no SchemaDiffer for database, caller will handle that
			return null;
		} else {
			// sorting entries by compatibility versions to ease selection of the highest among the smaller than database version
			// Note: we could have used the stream way of collecting things, but it's a bit less readable
			TreeMap<DatabaseSignet, SchemaDifferResolverEntry> SchemaDifferPerSortedCompatibility = new TreeMap<>(DatabaseSignet.COMPARATOR);
			databaseSchemaDiffers.forEach(SchemaDiffer -> SchemaDifferPerSortedCompatibility.merge(SchemaDiffer.getCompatibility(), SchemaDiffer, (c1, c2) -> {
				// we use same properties as DatabaseSignet comparator ones since we use a TreeMap based on it 
				String printableSignet = Strings.footPrint(c1.getCompatibility(), DatabaseSignet::toString);
				throw new IllegalStateException("Multiple SchemaDiffers with same database compatibility found : " + printableSignet);
			}));
			
			// we select the highest SchemaDiffer among the smaller than database version
			Entry<DatabaseSignet, SchemaDifferResolverEntry> foundEntry = SchemaDifferPerSortedCompatibility.floorEntry(databaseSignet);
			return foundEntry == null ? null : foundEntry.getValue();
		}
	}
}
