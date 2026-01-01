Jumper is a Java library for **upgrading database schemas** and enables **comparing database schemas** across different database vendors.

## Use Cases

- **Schema Evolution**: Apply the expected new database structure of your application
- **Schema Validation**: Ensure your production database schema matches your expected schema definition

### Cross-Vendor Migration
Compare schemas when migrating from one database vendor to another.

As such, it is a direct competitor to [Liquibase](https://github.com/liquibase/liquibase) and [Flyway](https://github.com/flyway/flyway). The main difference is that Jumper defines upgrades programmatically via a Java DSL, rather than using XML, JSON, YAML or direct SQL.
While Jumper's approach is conceptually closer to Liquibase than to Flyway, it provides greater customization capabilities:
- De facto, running Java code is made easier during the upgrade process (using ORMs, generating dynamic SQL, collecting remote data, etc.)
- Schema comparison can be customized to compare more database objects or check specific properties

## Overview

Through its [core module](core), Jumper's main purpose is to upgrade database schemas by executing structures defined via a [Java DSL](core/src/main/java/org/codefilarete/jumper/ddl/dsl), whose entry point is [DDLEase](core/src/main/java/org/codefilarete/jumper/ddl/dsl/DDLEase.java)

It also contains the [difference module](difference) designed to collect and compare database schema structures. It helps developers identify differences in tables, columns, indexes, foreign keys, and other database objects, making it ideal for:

- **Schema validation** between development, staging, and production environments
- **Database migration** verification
- **Multi-vendor database** synchronization analysis
- **Schema evolution** tracking

## Features

### Schema upgrade 📈

- ⛓ **Type-Safe Java DSL**: You define your schema changes (tables, columns, foreign keys, etc.) using fluent Java code and benefit from code completion.
- 🔢 **Versioned ChangeSets**: Modifications are organized into ChangeSet objects, which are then grouped into a FluentChangeLog. This structure helps track and order evolution steps.
- 🎦 **Tracked Applied Modifications**: Executed ChangeSets are stored in a database table (by default) to avoid re-applying the same modifications.


### Schema comparison 🔂
- 🔍 **Comprehensive Schema Collection**: Extracts complete database schemas including tables, columns, indexes, foreign keys, and constraints
- 🔁 **Cross-Database Comparison**: Compare schemas from the same or different database vendors
- 🔧 **Customizable Comparison Engine**: The comparison engine uses a fluent DSL that lets you create your own [SchemaDiffer](difference/src/main/java/org/codefilarete/jumper/schema/difference/SchemaDiffer.java) implementation

### Multi-vendor support 🎯
The following database vendors are currently supported:
- Derby
- H2
- HSQLDB
- MariaDB
- MySQL
- PostgreSQL

## Getting Started

### Maven Dependency

```xml
<dependency>
    <groupId>org.codefilarete.jumper</groupId>
    <artifactId>difference-postgresql-adapter</artifactId>
    <version>${jumper.version}</version>
</dependency>
```

*(Replace `postgresql` with your target database: `mysql`, `mariadb`, `h2`, `hsqldb`, or `derby`)*

### Basic Usage

#### Upgrading database structures
Several static methods are available from the [DDLEase](core/src/main/java/org/codefilarete/jumper/ddl/dsl/DDLEase.java) class to create a `FluentChangeLog` from a list of `ChangeSet`s.

```java
FluentChangeLog countryChangeLog = changeLog(
    changeSet("country_tables",
         createTable("Country")
             .addColumn("id", "BIGINT").primaryKey()
             .addColumn("name", "VARCHAR(255)")
             .addColumn("capitalId", "BIGINT")
             .addColumn("presidentId", "BIGINT"),
         createTable("City")
             .addColumn("id", "BIGINT").primaryKey()
             .addColumn("name", "VARCHAR(255)"),
         createTable("Person")
             .addColumn("id", "BIGINT").primaryKey()
             .addColumn("firstname", "VARCHAR(255)")
             .addColumn("lastname", "VARCHAR(255)"),
         createForeignKey("FK_Country_Capital", "Country", "City"),
         createForeignKey("FK_Country_President", "Country", "Person")
    ),
    changeSet("hibernate_tables",
         createTable("IdGenerator")
             .addColumn("sequence_name", "VARCHAR(255)")
             .addColumn("next_val", "BIGINT"))
);
countryChangeLog.applyTo(dataSource);
```

#### Comparing database structures

Let's compare two PostgreSQL databases by leveraging the [PostgreSQLSchemaElementCollector](difference/src/main/java/org/codefilarete/tool/jdbc/postgresql/PostgreSQLSchemaElementCollector.java) and [PostgreSQLSchemaDiffer](difference/src/main/java/org/codefilarete/tool/jdbc/postgresql/PostgreSQLSchemaDiffer.java) classes:

```java
// Connect to first database
PGSimpleDataSource dataSource1 = new PGSimpleDataSource();
dataSource1.setUrl("jdbc:postgresql://localhost:5432/database1");
dataSource1.setUser("admin");
dataSource1.setPassword("admin");

// Collect schema from first database
DefaultSchemaElementCollector collector1 = new PostgreSQLSchemaElementCollector(dataSource1.getConnection().getMetaData());
collector1
    .withCatalog("postgres")
    .withSchema("schema1")
    .withTableNamePattern("%");

// Connect to second database
PGSimpleDataSource dataSource2 = new PGSimpleDataSource();
dataSource2.setUrl("jdbc:postgresql://localhost:5432/database2");
dataSource2.setUser("admin");
dataSource2.setPassword("admin");

// Collect schema from second database
DefaultSchemaElementCollector collector2 = new PostgreSQLSchemaElementCollector(dataSource2.getConnection().getMetaData());
collector2
    .withCatalog("postgres")
    .withSchema("schema2")
    .withTableNamePattern("%");

// Compare and print differences to the console
PostgreSQLSchemaDiffer schemaDiffer = new PostgreSQLSchemaDiffer();
schemaDiffer.compareAndPrint(collector1.collect(), collector2.collect());
```

By default, Jumper analyzes and compares:

- **Tables**: Names, types, and presence
- **Columns**: Names, data types, nullability, default values, size/precision
- **Primary Keys**: Column composition and naming
- **Foreign Keys**: Relationships, referential actions, column mappings
- **Indexes**: Column composition, uniqueness, ordering (ASC/DESC)
- **Constraints**: Check constraints, unique constraints

## Project Structure
jumper/
├── core/ # Core schema collection functionality
├── difference/ # Schema comparison engine
└── difference-adapter/ # Database-specific adapters
    ├──difference-derby-adapter/
    ├── difference-h2-adapter/
    ├── difference-hsqldb-adapter/
    ├── difference-mariadb-adapter/
    ├── difference-mysql-adapter/
    └── difference-postgresql-adapter/

## Requirements

- Java 8 or higher
- JDBC drivers for your target database(s)
