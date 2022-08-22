package org.codefilarete.jumper.ddl.engine;

import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.impl.DDLChange;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.exception.NotImplementedException;

/**
 * @author Guillaume Mary
 */
public class Dialect {
	
	private final NewTableGenerator newTableHandler;
	private final NewForeignKeyGenerator newForeignKeyHandler;
	private final NewIndexGenerator newIndexHandler;
	private final DropTableGenerator dropTableHandler;
	
	public Dialect() {
		this(new NewTableHandler(), new NewForeignKeyHandler(), new NewIndexHandler(), new DropTableHandler());
	}
	
	private Dialect(NewTableGenerator newTableHandler,
				   NewForeignKeyGenerator newForeignKeyHandler,
				   NewIndexGenerator newIndexHandler,
				   DropTableGenerator dropTableHandler) {
		this.newTableHandler = newTableHandler;
		this.newForeignKeyHandler = newForeignKeyHandler;
		this.newIndexHandler = newIndexHandler;
		this.dropTableHandler = dropTableHandler;
	}
	
	public List<String> generateScript(DDLChange change) {
		return change.getDdlStatements().stream().map(this::generateScript).collect(Collectors.toList());
	}
	
	@VisibleForTesting
	String generateScript(DDLStatement ddlStatement) {
		String ddl;
		if (ddlStatement instanceof NewTable) {
			ddl = newTableHandler.generateScript((NewTable) ddlStatement);
		} else if (ddlStatement instanceof DropTable) {
			ddl = dropTableHandler.generateScript((DropTable) ddlStatement);
		} else if (ddlStatement instanceof NewForeignKey) {
			ddl = newForeignKeyHandler.generateScript(((NewForeignKey) ddlStatement));
		} else if (ddlStatement instanceof NewIndex) {
			ddl = newIndexHandler.generateScript(((NewIndex) ddlStatement));
		} else {
			throw new NotImplementedException("Statement of type " + Reflections.toString(ddlStatement.getClass()) + " is not supported");
		}
		return ddl;
	}
	
	public static class DialectBuilder {
		
		private NewTableGenerator newTableHandler;
		private NewForeignKeyGenerator newForeignKeyHandler;
		private NewIndexGenerator newIndexHandler;
		private DropTableGenerator dropTableHandler;
		
		public DialectBuilder() {
		}
		
		public DialectBuilder(Dialect dialect) {
			this.newTableHandler = dialect.newTableHandler;
			this.newForeignKeyHandler = dialect.newForeignKeyHandler;
			this.newIndexHandler = dialect.newIndexHandler;
			this.dropTableHandler = dialect.dropTableHandler;
		}
		
		
		
		public DialectBuilder withNewTableHandler(NewTableGenerator newTableHandler) {
			this.newTableHandler = newTableHandler;
			return this;
		}
		
		public DialectBuilder withNewForeignKeyHandler(NewForeignKeyGenerator newForeignKeyHandler) {
			this.newForeignKeyHandler = newForeignKeyHandler;
			return this;
		}
		
		public DialectBuilder withNewIndexHandler(NewIndexGenerator newIndexHandler) {
			this.newIndexHandler = newIndexHandler;
			return this;
		}
		
		public DialectBuilder withDropTableHandler(DropTableGenerator dropTableHandler) {
			this.dropTableHandler = dropTableHandler;
			return this;
		}
		
		Dialect build() {
			return new Dialect(newTableHandler, newForeignKeyHandler, newIndexHandler, dropTableHandler);
		}
	}
}
