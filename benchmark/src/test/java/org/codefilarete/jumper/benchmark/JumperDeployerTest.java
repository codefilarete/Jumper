package org.codefilarete.jumper.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.DiffGeneratorFactory;
import liquibase.diff.DiffResult;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.output.report.DiffToReport;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.structure.core.Column;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Index;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;
import liquibase.structure.core.View;
import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ChangeSetRunner;
import org.codefilarete.jumper.schema.MariaDBSchemaElementCollector;
import org.codefilarete.jumper.schema.difference.MariaDBSchemaDiffer;
import org.codefilarete.jumper.schema.metadata.MariaDBMetadataReader;
import org.codefilarete.tool.trace.Chrono;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.codefilarete.jumper.ChangeSet.changeSet;
import static org.codefilarete.jumper.ddl.dsl.DDLEase.*;
import static org.codefilarete.tool.collection.Arrays.asSet;

class JumperDeployerTest {
	
	private static final String JUMPER_SCHEMA_NAME = "Jumper";
	private static final String LIQUIBASE_SCHEMA_NAME = "Liquibase";
	private static MariaDBContainer MARIADB_CONTAINER;
	
	@BeforeAll
	static void startContainers() {
		System.out.println("Starting containers");
		MARIADB_CONTAINER = buildContainer(JUMPER_SCHEMA_NAME);
		MARIADB_CONTAINER.start();
	}
	
	private static MariaDBContainer buildContainer(String schemaName) {
		return (MariaDBContainer) new MariaDBContainer(DockerImageName.parse("mariadb:10.4"))
				.withUsername("root")
				.withPassword("")
//				.withDatabaseName(schemaName)
				.withConnectTimeoutSeconds(20);
	}
	
	@AfterAll
	static void stopContainers() {
		System.out.println("Stopping containers");
		MARIADB_CONTAINER.stop();
	}
	
	@Test
	void benchmark() throws SQLException, LiquibaseException, IOException {
		MariaDbDataSource dataSource = new MariaDbDataSource(MARIADB_CONTAINER.getJdbcUrl());
		dataSource.setUser(MARIADB_CONTAINER.getUsername());
		dataSource.setPassword(MARIADB_CONTAINER.getPassword());
		try (Connection connection = dataSource.getConnection()) {
			connection.createStatement().execute("create schema " + JUMPER_SCHEMA_NAME);
			connection.createStatement().execute("create schema " + LIQUIBASE_SCHEMA_NAME);
		}
		
		MariaDbDataSource liquibaseDataSource = new MariaDbDataSource(MARIADB_CONTAINER.withDatabaseName(LIQUIBASE_SCHEMA_NAME).getJdbcUrl());
		liquibaseDataSource.setUser(MARIADB_CONTAINER.getUsername());
		liquibaseDataSource.setPassword(MARIADB_CONTAINER.getPassword());
		deployWithLiquibase(liquibaseDataSource.getConnection());
		MariaDbDataSource jumperDataSource = new MariaDbDataSource(MARIADB_CONTAINER.withDatabaseName(JUMPER_SCHEMA_NAME).getJdbcUrl());
		jumperDataSource.setUser(MARIADB_CONTAINER.getUsername());
		jumperDataSource.setPassword(MARIADB_CONTAINER.getPassword());
		deployWithJumper(jumperDataSource.getConnection());
		
		// Diff
		
		MariaDBSchemaDiffer mariaDBSchemaDiffer = new MariaDBSchemaDiffer();
		MariaDBSchemaElementCollector mariaDBSchemaElementCollector = new MariaDBSchemaElementCollector(new MariaDBMetadataReader(liquibaseDataSource.getConnection().getMetaData()));
		mariaDBSchemaElementCollector.withSchema(LIQUIBASE_SCHEMA_NAME);
		MariaDBSchemaElementCollector mariaDBSchemaElementCollector1 = new MariaDBSchemaElementCollector(new MariaDBMetadataReader(jumperDataSource.getConnection().getMetaData()));
		mariaDBSchemaElementCollector1.withSchema(JUMPER_SCHEMA_NAME);
		Chrono c = new Chrono();
		mariaDBSchemaDiffer.compareAndPrint(mariaDBSchemaElementCollector.collect(), mariaDBSchemaElementCollector1.collect());
		System.out.println("Time spent in comparison : " + c);
		
		Chrono c2 = new Chrono();
		testSchemaDifference();
		System.out.println("Time spent in comparison : " + c2);
	}
	
	void testSchemaDifference() throws SQLException, LiquibaseException, IOException {
		MariaDbDataSource liquibaseDataSource = new MariaDbDataSource(MARIADB_CONTAINER.withDatabaseName(LIQUIBASE_SCHEMA_NAME).getJdbcUrl());
		liquibaseDataSource.setUser(MARIADB_CONTAINER.getUsername());
		liquibaseDataSource.setPassword(MARIADB_CONTAINER.getPassword());
		deployWithLiquibase(liquibaseDataSource.getConnection());
		MariaDbDataSource jumperDataSource = new MariaDbDataSource(MARIADB_CONTAINER.withDatabaseName(JUMPER_SCHEMA_NAME).getJdbcUrl());
		jumperDataSource.setUser(MARIADB_CONTAINER.getUsername());
		jumperDataSource.setPassword(MARIADB_CONTAINER.getPassword());
		Database hibernateDatabase = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(jumperDataSource.getConnection()));
		
		Database liquibaseDatabase = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(liquibaseDataSource.getConnection()));
		
		DiffResult result = this.getDiffResult(hibernateDatabase, liquibaseDatabase);
		
		if (!result.areEqual()) {
			// we get difference details because Liquibase doesn't print it
			ByteArrayOutputStream differences = new ByteArrayOutputStream();
			new DiffToReport(result, new PrintStream(differences)).print();
			System.out.println(differences);
		}
	}
	
	private DiffResult getDiffResult(Database dbHb, Database dbLiqui) throws LiquibaseException {
		// WARN : we recreate a Set clone to avoid a "UnsupportedOperationException" from StandardDiffGenerator line 45
		CompareControl compareControl = new CompareControl(new HashSet<>(Collections.unmodifiableSet(asSet(
				// WARN: we have to declare types and their subtypes because comparison doesn't take inheritance into account
				Column.class,
				ForeignKey.class,
				Index.class,
				PrimaryKey.class,
				UniqueConstraint.class,
				Sequence.class,
				// Unsupported, cf http://www.liquibase.org/documentation/diff.html
//       StoredProcedure.class,
				Table.class,
				View.class))));
		
		DiffResult result = DiffGeneratorFactory.getInstance().compare(dbHb, dbLiqui, compareControl);
		
		// we remove Liquibase table (not filtered by DiffResult even if it's a Liquibase class)
		SortedSet<String> liquibaseTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		liquibaseTables.addAll(Arrays.asList("databasechangelog", "databasechangeloglock"));
		// WARN : we must use getUnexpectedObjects() *without argument*, not the ones with argument
		// because they return a copy on which there's no need to remove items
//		removeObjectsRelatedToTable(result.getUnexpectedObjects(), table -> liquibaseTables.contains(table.getName()));
//		removeIgnoredChanges(result.getChangedObjects());
		return result;
	}
	
	void deployWithLiquibase(Connection connection) throws LiquibaseException {
		Chrono chrono = new Chrono();
		Liquibase liquibase = new Liquibase(
				"classpath:/survey/changeLog.xml",
				new ClassLoaderResourceAccessor(),
				new JdbcConnection(connection));
		liquibase.update(new Contexts());
		System.out.println(chrono);
	}
	
	void deployWithJumper(Connection connection) throws SQLException {
		Chrono chrono = new Chrono();
		
		Stream<ChangeSet> changeSetStream = Stream.of(
				changeSet("hibernate_tables",
						createTable("IdGenerator")
								.addColumn("sequence_name", "VARCHAR(255)")
								.addColumn("next_val", "BIGINT")),
				changeSet("questionnaire_tables",
						createTable("Questionnaire")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("name", "VARCHAR(255)"),
						createTable("QuestionnaireElement")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("questionnaire_id", "BIGINT")
								.addColumn("idx", "INT"),
						createTable("Question")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("label", "VARCHAR(255)")),
				changeSet("open_questions_tables",
						createTable("CommentQuestion")
								.addColumn("id", "BIGINT").primaryKey(),
						createTable("NumericQuestion")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("`precision`", "INT").notNull()
								.addColumn("`scale`", "INT").notNull(),
						createTable("DateQuestion")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("withDay", "BIT(1)").notNull()
								.addColumn("withHour", "BIT(1)").notNull()
								.addColumn("withMillisecond", "BIT(1)").notNull()
								.addColumn("withMinute", "BIT(1)").notNull()
								.addColumn("withMonth", "BIT(1)").notNull()
								.addColumn("withYear", "BIT(1)").notNull()),
				changeSet("closed_questions_tables",
						createTable("ClosedQuestion")
								.addColumn("id", "BIGINT").primaryKey(),
						createTable("SingleChoiceQuestion")
								.addColumn("id", "BIGINT").primaryKey(),
						createTable("MultipleChoiceQuestion")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("maximumChoiceCount", "INT").notNull()
								.addColumn("minimumChoiceCount", "INT").notNull()
								.addColumn("ordered", "BIT(1)").notNull(),
						createTable("Choice")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("label", "VARCHAR(255)")
								.addColumn("weight", "DOUBLE").notNull(),
						createTable("ClosedQuestion_Choice")
								.addColumn("ClosedQuestion_id", "BIGINT")
								.addColumn("choices_id", "BIGINT").notNull()
								.addColumn("idx", "INT").notNull()
								.primaryKey("ClosedQuestion_id", "idx")),
				changeSet("extra_questionnaireElements_tables",
						createTable("Section")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("title", "VARCHAR(255)"),
						createTable("QuestionTable")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("label", "VARCHAR(255)"),
						createTable("Criterion")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("label", "VARCHAR(255)"),
						createTable("QuestionTable_Criterion")
								.addColumn("QuestionTable_id", "BIGINT").notNull()
								.addColumn("criteria_id", "BIGINT").notNull(),
						createTable("QuestionTable_Question")
								.addColumn("QuestionTable_id", "BIGINT").notNull()
								.addColumn("questions_id", "BIGINT").notNull()),
				changeSet("questionnaire_constraints",
						createForeignKey("FK2tfanb3djtqnhdiql7to60m0",
								"MultipleChoiceQuestion").addSourceColumn("id")
								.targetTable("ClosedQuestion").addTargetColumn("id"),
						createForeignKey("FK3mmw0crbasp96o5nj4a1or7fv",
								"SingleChoiceQuestion").addSourceColumn("id")
								.targetTable("ClosedQuestion").addTargetColumn("id"),
						createForeignKey("FK4g6g0kgcnr030wntye36jqrfy",
								"QuestionTable_Criterion").addSourceColumn("QuestionTable_id")
								.targetTable("QuestionTable").addTargetColumn("id"),
						createForeignKey("FK6korjoy8tarmfibrjivymnpbx",
								"ClosedQuestion_Choice").addSourceColumn("choices_id")
								.targetTable("Choice").addTargetColumn("id"),
						createForeignKey("FK7phnsqdg7oowleve6gd2grbel",
								"Question").addSourceColumn("id")
								.targetTable("QuestionnaireElement").addTargetColumn("id"),
						createForeignKey("FK9ibqw4128bwwg9kagxvg08r0n",
								"CommentQuestion").addSourceColumn("id")
								.targetTable("Question").addTargetColumn("id"),
						createForeignKey("FKak5sq2gxqxgoakx321oscegeg",
								"Section").addSourceColumn("id")
								.targetTable("QuestionnaireElement").addTargetColumn("id"),
						createForeignKey("FKba1ex7uov2011y45ic95i9ffq",
								"QuestionTable_Criterion").addSourceColumn("criteria_id")
								.targetTable("Criterion").addTargetColumn("id"),
						createForeignKey("FKi1rbx6r3j761ak2dttl4ggvpx",
								"ClosedQuestion_Choice").addSourceColumn("ClosedQuestion_id")
								.targetTable("ClosedQuestion").addTargetColumn("id"),
						createForeignKey("FKjf4gtlxichnmo9q6kdhm08sq8",
								"QuestionnaireElement").addSourceColumn("questionnaire_id")
								.targetTable("Questionnaire").addTargetColumn("id"),
						createForeignKey("FKlklb7ywnghhfy9ovuvnyb44ao",
								"QuestionTable_Question").addSourceColumn("QuestionTable_id")
								.targetTable("QuestionTable").addTargetColumn("id"),
						createForeignKey("FKn4diyjvls00btbs5rwqs7bu11",
								"QuestionTable_Question").addSourceColumn("questions_id")
								.targetTable("Question").addTargetColumn("id"),
						createForeignKey("FKnk2fdd1p7qulqh8k6bilo0bto",
								"QuestionTable").addSourceColumn("id")
								.targetTable("QuestionnaireElement").addTargetColumn("id"),
						createForeignKey("FKojtw674u2o0ahxrlj6gb2hmkf",
								"ClosedQuestion").addSourceColumn("id")
								.targetTable("Question").addTargetColumn("id"),
						createForeignKey("FKsbuw2skldt72n4sv77h10efp9",
								"NumericQuestion").addSourceColumn("id")
								.targetTable("Question").addTargetColumn("id"),
						createForeignKey("FKov1onp7y4ybu82wjcvqailbjn",
								"DateQuestion").addSourceColumn("id")
								.targetTable("Question").addTargetColumn("id"),
						createUniqueContraint("UK_9ily67sdsm62u8h9gocm28coy", "QuestionTable_Question", "questions_id"),
						createUniqueContraint("UK_amqe6fhn528v0o1m5yxpaa35w", "QuestionTable_Criterion", "criteria_id"),
						createUniqueContraint("UK_ikfrkeim0qy7ct2fqri242eki", "ClosedQuestion_Choice", "choices_id")),
				changeSet("answer_tables",
						createTable("Answer")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("DTYPE", "VARCHAR(13)").notNull()
								.addColumn("question_id", "BIGINT")
								.addColumn("form_id", "BIGINT").notNull()
								.addColumn("choice_id", "BIGINT")
								.addColumn("value", "double precision")
								.addColumn("date", "datetime(6)"),
						createTable("Answer_Choice")
								.addColumn("MultipleChoiceAnswer_id", "BIGINT").notNull()
								.addColumn("choices_id", "BIGINT").notNull()
								.addColumn("rank", "INT").notNull()
								.primaryKey("MultipleChoiceAnswer_id", "rank"),
						createForeignKey("FK2viobqwt4kki5q04i51fox7ax",
								"Answer").addSourceColumn("choice_id")
								.targetTable("Choice").addTargetColumn("id"),
						createForeignKey("FKc6i2fmg06305gbvmqqnxyxt2i",
								"Answer_Choice").addSourceColumn("MultipleChoiceAnswer_id")
								.targetTable("Answer").addTargetColumn("id"),
						createForeignKey("FKfiomvt17psxodcis3d8nmopx8",
								"Answer").addSourceColumn("question_id")
								.targetTable("Question").addTargetColumn("id"),
						createForeignKey("FKn3xsqrwvubdk3vyg4e8pjm3sd",
								"Answer_Choice").addSourceColumn("choices_id")
								.targetTable("Choice").addTargetColumn("id")),
						
				changeSet("distribution_tables",
						createTable("DistributionConfiguration")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("messageTemplate_id", "BIGINT"),
						createTable("Distribution")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("sendTime", "datetime(6)")
								.addColumn("configuration_id", "BIGINT")
								.addColumn("message_id", "BIGINT")
								.addColumn("personas_id", "BIGINT"),
						createTable("Persona")
								.addColumn("id", "BIGINT").primaryKey(),
						createTable("Message")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("content", "VARCHAR(255)"),
						createTable("ContactMean")
								.addColumn("id", "BIGINT").primaryKey()
								.addColumn("persona_id", "BIGINT").notNull()
								.addColumn("DTYPE", "VARCHAR(31)").notNull()
								.addColumn("phoneNumber", "VARCHAR(255)")
								.addColumn("email", "VARCHAR(255)"),
						createTable("DistributionConfiguration_ContactMean")
								.addColumn("DistributionConfiguration_id", "BIGINT").primaryKey()
								.addColumn("personas_id", "BIGINT").primaryKey(),
						createForeignKey("FK3dinoqtgbhiv0tx7fmtqwj7aw",
								"DistributionConfiguration").addSourceColumn("messageTemplate_id")
								.targetTable("Message").addTargetColumn("id"),
						createForeignKey("FK3ir0g80egqsx0w4g8gdjodorr",
								"Distribution").addSourceColumn("message_id")
								.targetTable("Message").addTargetColumn("id"),
						createForeignKey("FK5r24hv6bjr2sjk29upfh0vw09",
								"ContactMean").addSourceColumn("persona_id")
								.targetTable("Persona").addTargetColumn("id"),
						createForeignKey("FK6oirw3dw1561f15kr1vs2m7xd",
								"DistributionConfiguration_ContactMean").addSourceColumn("DistributionConfiguration_id")
								.targetTable("DistributionConfiguration").addTargetColumn("id"),
						createForeignKey("FK7th6op3ks1vt75bda36xj8pd6",
								"Distribution").addSourceColumn("personas_id")
								.targetTable("ContactMean").addTargetColumn("id"),
						createForeignKey("FKlo25lam49k7xwc32f6h5pw85y",
								"Distribution").addSourceColumn("configuration_id")
								.targetTable("DistributionConfiguration").addTargetColumn("id"),
						createForeignKey("FKo47m1faxdqlc7qtyg6834yb3o",
								"DistributionConfiguration_ContactMean").addSourceColumn("personas_id")
								.targetTable("ContactMean").addTargetColumn("id"),
						createUniqueContraint("UK_77tdfhlc0il375875o4epwfrd", "DistributionConfiguration_ContactMean", "personas_id"))
		);
		
		ChangeSetRunner.forJdbcStorage(() -> connection, changeSetStream)
				.processUpdate();
		System.out.println(chrono);
	}
}
