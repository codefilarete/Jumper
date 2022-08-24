package org.codefilarete.jumper.impl;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.NewTable.NewColumn;
import org.codefilarete.jumper.ddl.dsl.support.Table;
import org.codefilarete.jumper.impl.ChangeChecksumer.ByteBuffer;
import org.codefilarete.tool.exception.NotImplementedException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

class ChangeSetChecksumerTest {
	
	@Test
	void buildChecksum_handleNewTable() {
		ChangeChecksumer testInstance = Mockito.spy(new ChangeChecksumer());
		NewTable newTable = new NewTable("x");
		testInstance.buildChecksum(new ChangeSet("x", false).addChanges(newTable));
		verify(testInstance).giveSignature(newTable);
	}
	
	@Test
	void buildChecksum_handleDropTable() {
		ChangeChecksumer testInstance = Mockito.spy(new ChangeChecksumer());
		DropTable dropTable = new DropTable("x");
		testInstance.buildChecksum(new ChangeSet("x", false).addChanges(dropTable));
		verify(testInstance).giveSignature(dropTable);
	}
	
	@Test
	void buildChecksum_handleNewForeignKey() {
		ChangeChecksumer testInstance = Mockito.spy(new ChangeChecksumer());
		NewForeignKey newForeignKey = new NewForeignKey("x", new Table("y"));
		newForeignKey.addTargetColumn("a");
		newForeignKey.setTargetTable(new Table("b"));
		testInstance.buildChecksum(new ChangeSet("x", false).addChanges(newForeignKey));
		verify(testInstance).giveSignature(newForeignKey);
	}
	
	@Test
	void buildChecksum_handleNewIndex() {
		ChangeChecksumer testInstance = Mockito.spy(new ChangeChecksumer());
		NewIndex newIndex = new NewIndex("x", new Table("y"));
		testInstance.buildChecksum(new ChangeSet("x", false).addChanges(newIndex));
		verify(testInstance).giveSignature(newIndex);
	}
	
	@Test
	void giveSignature_handleNewTable() {
		ChangeChecksumer testInstance = new ChangeChecksumer();
		NewTable newTable = new NewTable("x");
		newTable.addColumn(new NewColumn("a", "int").autoIncrement());
		newTable.addColumn(new NewColumn("b", "varchar").notNull());
		newTable.addColumn(new NewColumn("c", "blob").setDefaultValue("42"));
		newTable.addUniqueConstraint("xyz", "x", "y", "z");
		newTable.addUniqueConstraint("ab", "a", "b");
		newTable.setPrimaryKey("a", "b", "c");
		String signature = testInstance.giveSignature(newTable);
		assertThat(signature).isEqualTo("NewTable" +
				" a int true null true null," +
				" b varchar false null false null," +
				" c blob true 42 false null" +
				" PK a b c" +
				" UK xyz x y z," +
				" UK ab a b"
		);
	}
	
	@Test
	void giveSignature_handleDropTable() {
		ChangeChecksumer testInstance = new ChangeChecksumer();
		DropTable dropTable = new DropTable("x");
		String signature = testInstance.giveSignature(dropTable);
		assertThat(signature).isEqualTo("DropTable x");
	}
	
	@Test
	void giveSignature_handleNewForeignKey() {
		ChangeChecksumer testInstance = new ChangeChecksumer();
		NewForeignKey newForeignKey = new NewForeignKey("X->Y", new Table("X"));
		newForeignKey.addSourceColumn("x1");
		newForeignKey.addSourceColumn("x2");
		newForeignKey.setTargetTable(new Table("Y"));
		newForeignKey.addTargetColumn("y1");
		newForeignKey.addTargetColumn("y2");
		String signature = testInstance.giveSignature(newForeignKey);
		assertThat(signature).isEqualTo("NewForeignKey X->Y X x1 x2, Y y1 y2");
	}
	
	@Test
	void giveSignature_handleNewIndex() {
		ChangeChecksumer testInstance = new ChangeChecksumer();
		NewIndex newIndex = new NewIndex("x", new Table("y"));
		String signature = testInstance.giveSignature(newIndex);
		assertThat(signature).isEqualTo("NewIndex x y false");
	}
	
	@Test
	void giveSignature() {
		ChangeChecksumer testInstance = new ChangeChecksumer();
		Assertions.assertThatCode(() -> {
			testInstance.giveSignature(new SupportedChange() {
			});
		}).isInstanceOf(NotImplementedException.class)
				.hasMessageStartingWith("Signature computation is not implemented for ");
	}
	
	@Nested
	class ByteBufferTest {
		
		@Test
		void append_autoExpands() {
			ByteBuffer testInstance = new ByteBuffer(10);
			testInstance.append("Hello".getBytes(StandardCharsets.UTF_8));
			assertThat(testInstance.getBytes()).asString(StandardCharsets.UTF_8).isEqualTo("Hello");
			testInstance.append(" world !".getBytes(StandardCharsets.UTF_8));
			assertThat(testInstance.getBytes()).asString(StandardCharsets.UTF_8).isEqualTo("Hello world !");
		}
		
	}
}