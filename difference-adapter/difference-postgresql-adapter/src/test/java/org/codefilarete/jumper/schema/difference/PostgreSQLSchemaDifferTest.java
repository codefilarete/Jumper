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
import org.codefilarete.jumper.schema.PostgreSQLDataSource;
import org.codefilarete.jumper.schema.PostgreSQLSchemaElementCollector;
import org.codefilarete.jumper.schema.PostgreSQLTest;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator;
import org.codefilarete.jumper.schema.difference.SchemaDiffer.ComparisonChain.PropertyComparator.PropertyDiff;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgreSQLSchemaDifferTest extends PostgreSQLTest {
	
	@Test
	void compare() throws SQLException {
		UrlAwareDataSource dataSource = new PostgreSQLDataSource(postgresql);
		Connection connection1 = dataSource.getConnection();
		connection1.setAutoCommit(false);	// autocommit is true by default
		connection1.prepareStatement("create schema REFERENCE").execute();
		connection1.prepareStatement("set schema 'reference'").execute();
		connection1.prepareStatement("create table A(id BIGSERIAL, name VARCHAR(200) not null, age float, primary key (id))").execute();
		connection1.prepareStatement("create table B(id BIGINT, aId BIGINT, dummyData VARCHAR(50), primary key (id), constraint fromBtoA foreign key (aId) references A(id))").execute();
		connection1.prepareStatement("create table C(id BIGINT, aId BIGINT, lastname VARCHAR(50), primary key (id), constraint fromCtoA foreign key (aId) references A(id))").execute();
		connection1.prepareStatement("create table D(firstname VARCHAR(50))").execute();
		connection1.prepareStatement("create unique index toto on A(name asc)").execute();
		connection1.prepareStatement("create unique index tata on C(lastname desc)").execute();
		connection1.prepareStatement("create view TUTU as select a.id, a.name, b.dummyData from A a inner join B b on a.id = b.aId").execute();
		connection1.commit();
		connection1.close();
		
		Connection connection2 = dataSource.getConnection();
		connection2.setAutoCommit(false);	// autocommit is true by default
		connection2.prepareStatement("create schema COMPARISON").execute();
		connection2.prepareStatement("set schema 'comparison'").execute();
		connection2.prepareStatement("create table A(id BIGSERIAL, name VARCHAR(200) not null, age float, primary key (id))").execute();
		connection2.prepareStatement("create table B(id BIGINT, aId BIGINT, dummyData VARCHAR(50), primary key (id), constraint fromBtoA foreign key (aId) references A(id))").execute();
		connection2.prepareStatement("create table C(id BIGINT, aId BIGINT, firstname VARCHAR(50), lastname VARCHAR(100), primary key (id))").execute();
		connection2.prepareStatement("create index tata on C(lastname asc)").execute();
		connection2.prepareStatement("create view TUTU as select a.id, a.name, b.dummyData from A a inner join B b on a.id = b.aId").execute();
		connection2.commit();
		connection2.close();
		
		DefaultSchemaElementCollector schemaElementCollector = new PostgreSQLSchemaElementCollector(dataSource.getConnection().getMetaData());
		schemaElementCollector
				.withCatalog(null)
				.withSchema("reference")
				.withTableNamePattern("%");
		Schema ddlElements1 = schemaElementCollector.collect();
		
		schemaElementCollector
				.withCatalog(null)
				.withSchema("comparison")
				.withTableNamePattern("%");
		Schema ddlElements2 = schemaElementCollector.collect();
		
		SchemaDiffer testInstance = new PostgreSQLSchemaDiffer();
		Set<AbstractDiff<?>> diffs = testInstance.compare(ddlElements1, ddlElements2);
		
		System.out.println("----------------------------------------------------------");
		
		System.out.println("Added in " + dataSource.getUrl());
		Map<String, Map<String, Column>> columnsPerTableNameAndColumnName = ddlElements2.getTables().stream().collect(
				Collectors.toMap(Table::getName, t -> t.getColumns().stream().collect(
						Collectors.toMap(Column::getName, Function.identity()))));
		Column column = columnsPerTableNameAndColumnName.get("c").get("firstname");
		assertThat(diffs.stream().filter(d -> d.getState() == State.ADDED)
				.map(AbstractDiff::getReplacingInstance).filter(Column.class::isInstance).map(Column.class::cast))
				.containsExactly(column);
		
		diffs.stream().filter(d -> d.getState() == State.ADDED).forEach(d -> {
			System.out.println(d.getReplacingInstance());
		});
		
		System.out.println("Modifications between " + dataSource.getUrl() + " and " + dataSource.getUrl());
		diffs.stream().filter(d -> d.getState() == State.HELD).forEach(d -> {
			if (d instanceof PropertyComparator.PropertyDiff) {
				String propertyName = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(((PropertyDiff<?, ?>) d).getPropertyAccessor())).getName();
				System.out.println(propertyName + ": " + d.getSourceInstance() + " vs " + d.getReplacingInstance());
			}
		});
		assertThat(diffs.stream().filter(d -> d.getState() == State.HELD).filter(PropertyDiff.class::isInstance).map(PropertyDiff.class::cast)
				.map(propertyDiff -> {
					AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(((PropertyDiff<?, ?>) propertyDiff).getPropertyAccessor()));
					String propertyName = accessorDefinition.getName();
					return accessorDefinition.getDeclaringClass().getSimpleName() + "." + propertyName + ": "
							+ propertyDiff.getSourceInstance() + " vs " + propertyDiff.getReplacingInstance();
				})).containsExactlyInAnyOrder(
				"Index.unique: Index{name='tata', unique=true, columns=lastname} vs Index{name='tata', unique=false, columns=lastname}",
				"Column.size: Column{tableName='c', name='lastname', type='VARCHAR', size=50} vs Column{tableName='c', name='lastname', type='VARCHAR', size=100}"
		);
		
		System.out.println("Missing in " + dataSource.getUrl());
		Map<String, Table> tablesPerName = ddlElements1.getTables().stream().collect(
				Collectors.toMap(Table::getName, Function.identity()));
		assertThat(diffs.stream().filter(d -> d.getState() == State.REMOVED)
				.map(AbstractDiff::getSourceInstance).filter(Table.class::isInstance).map(Table.class::cast))
				.containsExactly(tablesPerName.get("d"));
		Map<String, Index> indexesPerName = ddlElements1.getIndexes().stream().collect(
				Collectors.toMap(Index::getName, Function.identity()));
		assertThat(diffs.stream().filter(d -> d.getState() == State.REMOVED)
				.map(AbstractDiff::getSourceInstance).filter(Index.class::isInstance).map(Index.class::cast))
				.contains(indexesPerName.get("toto"));
		
		diffs.stream().filter(d -> d.getState() == State.REMOVED).forEach(d -> {
			System.out.println(d.getSourceInstance());
		});
	}
	
}