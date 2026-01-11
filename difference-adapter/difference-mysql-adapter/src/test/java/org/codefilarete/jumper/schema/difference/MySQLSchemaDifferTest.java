package org.codefilarete.jumper.schema.difference;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.jumper.schema.DefaultSchemaElementCollector;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Index;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table;
import org.codefilarete.jumper.schema.DefaultSchemaElementCollector.Schema.Table.Column;
import org.codefilarete.jumper.schema.MySQLDataSource;
import org.codefilarete.jumper.schema.MySQLSchemaElementCollector;
import org.codefilarete.jumper.schema.MySQLTest;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator.PropertyDiff;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MySQLSchemaDifferTest extends MySQLTest {
	
	@Test
	void compare() throws SQLException {
		UrlAwareDataSource dataSource = new MySQLDataSource(mysqldb);
		Connection connection1 = dataSource.getConnection();
		connection1.setAutoCommit(false);	// autocommit is true by default
		connection1.prepareStatement("create schema REFERENCE").execute();
		connection1.prepareStatement("use REFERENCE").execute();
		connection1.prepareStatement("create table A(id BIGINT auto_increment, name VARCHAR(200) not null, age float, primary key (id))").execute();
		connection1.prepareStatement("create table B(id BIGINT, aId BIGINT, dummyData VARCHAR(50), primary key (id), constraint fromBtoA foreign key (aId) references A(id))").execute();
		connection1.prepareStatement("create table C(id BIGINT, aId BIGINT, lastname VARCHAR(50), primary key (id), constraint fromCtoA foreign key (aId) references A(id))").execute();
		connection1.prepareStatement("create table D(firstname VARCHAR(50))").execute();
		connection1.prepareStatement("create unique index toto on A(name asc)").execute();
		connection1.prepareStatement("create unique index tata on C(lastname desc)").execute();
		connection1.prepareStatement("create view TUTU as select a.id, a.name, b.dummyData from A a inner join B b on a.id = b.aId").execute();
		connection1.commit();
		connection1.close();
		
		Connection connection2 = dataSource.getConnection();
		connection2.setAutoCommit(false);
		connection2.prepareStatement("create schema COMPARISON").execute();
		connection2.prepareStatement("use COMPARISON").execute();
		connection2.prepareStatement("create table A(id BIGINT auto_increment, name VARCHAR(200) not null, age float, primary key (id))").execute();
		connection2.prepareStatement("create table B(id BIGINT, aId BIGINT, dummyData VARCHAR(50), primary key (id), constraint fromBtoA foreign key (aId) references A(id))").execute();
		connection2.prepareStatement("create table C(id BIGINT, aId BIGINT, firstname VARCHAR(50), lastname VARCHAR(100), primary key (id))").execute();
		connection2.prepareStatement("create table E(firstname VARCHAR(50))").execute();
		connection2.prepareStatement("create index tata on C(lastname asc)").execute();
		connection2.prepareStatement("create view TUTU as select a.id, a.name, b.dummyData from A a inner join B b on a.id = b.aId").execute();
		connection2.commit();
		connection2.close();
		
		DefaultSchemaElementCollector schemaElementCollector = new MySQLSchemaElementCollector(dataSource.getConnection().getMetaData());
		schemaElementCollector
				.withCatalog(null)
				.withSchema("REFERENCE")
				.withTableNamePattern("%");
		Schema ddlElements1 = schemaElementCollector.collect();
		
		schemaElementCollector
				.withCatalog(null)
				.withSchema("COMPARISON")
				.withTableNamePattern("%");
		Schema ddlElements2 = schemaElementCollector.collect();
		
		SchemaDiffer testInstance = new MySQLSchemaDiffer();
		Set<AbstractDiff<?>> diffs = testInstance.compare(ddlElements1, ddlElements2);
		
		// Elements added in "COMPARISON" schema
		assertThat(diffs.stream().filter(d -> d.getState() == State.ADDED)).map(diff -> diff.getReplacingInstance().toString())
				.containsExactlyInAnyOrder(
						"Table{name='E'}",
						"Column{tableName='C', name='firstname', type='VARCHAR', size=50, scale=null, nullable=true}"
				);
		
		// Modifications between in "REFERENCE" and "COMPARISON" schemas
		assertThat(diffs.stream().filter(d -> d.getState() == State.HELD).filter(PropertyDiff.class::isInstance).map(PropertyDiff.class::cast)
				.map(propertyDiff -> {
					AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(((PropertyDiff<?, ?>) propertyDiff).getPropertyAccessor()));
					String propertyName = accessorDefinition.getName();
					return accessorDefinition.getDeclaringClass().getSimpleName() + "." + propertyName + ": "
							+ propertyDiff.getSourceInstance() + " vs " + propertyDiff.getReplacingInstance();
				})).containsExactlyInAnyOrder(
				"Index.unique: Index{name='tata', table='C', unique=true, columns={'lastname'}} vs Index{name='tata', table='C', unique=false, columns={'lastname'}}",
				"IndexedColumn.direction: IndexedColumn{index=tata, column=lastname, direction=DESC} vs IndexedColumn{index=tata, column=lastname, direction=ASC}",
				"Column.size: Column{tableName='C', name='lastname', type='VARCHAR', size=50, scale=null, nullable=true} vs Column{tableName='C', name='lastname', type='VARCHAR', size=100, scale=null, nullable=true}"
		);
		
		// Missing elements in "COMPARISON" schema
		assertThat(diffs.stream().filter(d -> d.getState() == State.REMOVED)).map(diff -> diff.getSourceInstance().toString())
				.containsExactlyInAnyOrder(
						"Table{name='D'}",
						"Index{name='toto', table='A', unique=true, columns={'name'}}",
						"Index{name='fromCtoA', table='C', unique=false, columns={'aId'}}"
				);
	}
	
}