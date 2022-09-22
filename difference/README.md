# Module to compare database schema

Sometimes database schema must be compared to another one to check for difference, for instance it can be used to check that some update scripts give same result as the one expected by an ORM library.
This module tries to feel this need. It contains a main approach through [SchemaDiffer](src/main/java/org/codefilarete/jumper/schema/difference/SchemaDiffer.java) and [DefaultSchemaElementCollector](src/main/java/org/codefilarete/jumper/schema/DefaultSchemaElementCollector.java) but since some gap exist between database vendor, some adjustments must be done, so some adapters are required to collect database objects as well as comparing them since some attributes may not be supported by every vendor.

See [difference-adapter](../difference-adapter) submodules to see if one fits your need, or tweak one of them to implement your comparison, or even implement yours if none of them matches your need.
