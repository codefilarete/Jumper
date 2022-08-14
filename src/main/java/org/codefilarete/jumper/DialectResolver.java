package org.codefilarete.jumper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Comparator;

import org.codefilarete.jumper.ddl.engine.Dialect;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.exception.Exceptions;

/**
 * Simple contract to determine the {@link Dialect} to be used with a database
 * 
 * @author Guillaume Mary
 */
public interface DialectResolver {
	
	/**
	 * Expected to give the {@link Dialect} to be used with a database
	 * 
	 * @param databaseSignet a representation of a database
	 * @return the most compatible dialect with database
	 */
	Dialect determineDialect(DatabaseSignet databaseSignet);
	
	/**
	 * @author Guillaume Mary
	 */
	interface DialectResolverEntry {
		
		DatabaseSignet getCompatibility();
		
		Dialect getDialect();
	}
	
	/**
	 * Storage for database product and version.
	 */
	class DatabaseSignet {
		
		/**
		 * Builds a {@link DatabaseSignet} from a connection to create the database signature from its metadata.
		 * Could be a constructor but would require callers to handle {@link SQLException} which is quite boring, therefore this method handles it
		 * by wrapping it into a {@link RuntimeException}
		 *
		 * @param connection the connection from which a database signature must be created
		 * @return a new {@link DatabaseSignet}
		 */
		public static DatabaseSignet fromMetadata(Connection connection) {
			try {
				DatabaseMetaData databaseMetaData = connection.getMetaData();
				return new DatabaseSignet(databaseMetaData.getDatabaseProductName(), databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion());
			} catch (SQLException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
		
		public static Comparator<DatabaseSignet> COMPARATOR = Comparator
				.comparing(DatabaseSignet::getProductName)
				.thenComparingInt(DatabaseSignet::getMajorVersion)
				.thenComparingInt(DatabaseSignet::getMinorVersion);
		
		private final String productName;
		
		private final int majorVersion;
		
		private final int minorVersion;
		
		/**
		 * Constructor with mandatory elements.
		 * See {@link #fromMetadata(Connection)} to build one for a database.
		 *
		 * @param productName database product name, must be strictly equals to the one of database metadata, else detection algorithm will fail
		 * @param majorVersion database product major version, as the one given by database metadata
		 * @param minorVersion database product minor version, as the one given by database metadata
		 * @see #fromMetadata(Connection)
		 */
		@VisibleForTesting
		public DatabaseSignet(String productName, int majorVersion, int minorVersion) {
			this.productName = productName;
			this.majorVersion = majorVersion;
			this.minorVersion = minorVersion;
		}
		
		public String getProductName() {
			return productName;
		}
		
		public int getMajorVersion() {
			return majorVersion;
		}
		
		public int getMinorVersion() {
			return minorVersion;
		}
		
		/**
		 * Implemented as "product name X.Y". To be used for debug or simple printing.
		 *
		 * @return "product name X.Y"
		 */
		@Override
		public String toString() {
			return productName + " " + majorVersion + "." + minorVersion;
		}
	}
}
