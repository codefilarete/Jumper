package org.codefilarete.jumper.ddl.engine;

import java.util.List;

import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.Table;
import org.codefilarete.jumper.ddl.engine.Dialect.DialectBuilder;
import org.codefilarete.jumper.impl.DDLChange;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class DialectTest {
	
	static Object[][] generateScript() {
			return new Object[][] {
					{ new DialectBuilder().withNewTableHandler(table -> "my script"), new NewTable("toto"), "my script" },
					{ new DialectBuilder().withNewForeignKeyHandler(table -> "my script"), new NewForeignKey("toto", new Table("titi")), "my script" },
					{ new DialectBuilder().withNewIndexHandler(table -> "my script"), new NewIndex("toto", new Table("titi")), "my script" },
					{ new DialectBuilder().withDropTableHandler(table -> "my script"), new DropTable("toto"), "my script" }
			};
	}
	
	@ParameterizedTest
	@MethodSource
	void generateScript(DialectBuilder scriptGenerator, DDLStatement input, String expectedScript) {
		Dialect testInstance = scriptGenerator.build();
		List<String> script = testInstance.generateScript(new DDLChange("x", input));
		assertThat(script).containsExactly(expectedScript);
	}
}