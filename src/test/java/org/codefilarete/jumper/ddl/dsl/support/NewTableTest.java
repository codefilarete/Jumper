package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewColumn;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NewTableTest {
	
	@Test
	void addColumn_columnAlreadyDefined_throwsException() {
		NewTable testInstance = new NewTable("toto");
		testInstance.addColumn(new NewColumn("xyz", "int"));
		assertThatThrownBy(() -> testInstance.addColumn(new NewColumn("xyz", "int")))
				.isInstanceOf(DuplicateColumnDefinition.class)
				.hasMessageContaining("xyz");
	}
}