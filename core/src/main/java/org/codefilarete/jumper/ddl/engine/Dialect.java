package org.codefilarete.jumper.ddl.engine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.jumper.ddl.dsl.support.AddColumn;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.NewUniqueConstraint;
import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.exception.NotImplementedException;

/**
 * @author Guillaume Mary
 */
public class Dialect {
	
	private final Map<Class<? extends SupportedChange>, SQLGenerator<? extends SupportedChange>> generatorsPerSupportedChange = new HashMap<>();
	
	public Dialect() {
		this(Collections.emptySet());
	}
	
	public Dialect(Set<String> protectedSqlKeywords) {
		register(NewTable.class, new NewTableHandler(protectedSqlKeywords));
		register(NewForeignKey.class, new NewForeignKeyHandler());
		register(NewIndex.class, new NewIndexHandler());
		register(DropTable.class, new DropTableHandler());
		register(ModifyColumn.class, new ModifyColumnHandler(protectedSqlKeywords));
		register(AddColumn.class, new AddColumnHandler(protectedSqlKeywords));
		register(NewUniqueConstraint.class, new NewUniqueConstraintHandler(protectedSqlKeywords));
	}
	
	private <C extends SupportedChange> void register(Class<C> handledClass, SQLGenerator<? super C> sqlGenerator) {
		this.generatorsPerSupportedChange.put(handledClass, sqlGenerator);
	}
	
	public String generateScript(SupportedChange supportedChange) {
		SQLGenerator<? super SupportedChange> sqlGenerator = (SQLGenerator<? super SupportedChange>) this.generatorsPerSupportedChange.get(supportedChange.getClass());
		if (sqlGenerator == null) {
			throw new NotImplementedException("Statement of type " + Reflections.toString(supportedChange.getClass()) + " is not supported");
		}
		return sqlGenerator.generateScript(supportedChange);
	}
	
	public static class DialectBuilder {
		
		private final Dialect result = new Dialect();
		
		public DialectBuilder() {
		}
		
		public <C extends SupportedChange> DialectBuilder withSqlGenerator(Class<C> handledClass, SQLGenerator<? super C> sqlGenerator) {
			result.register(handledClass, sqlGenerator);
			return this;
		}
		
		Dialect build() {
			return result;
		}
	}
}
