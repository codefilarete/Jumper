package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.*;
import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.exception.NotImplementedException;

import static org.codefilarete.jumper.ddl.engine.NewTableHandler.MARIADB_KEYWORDS;

/**
 * @author Guillaume Mary
 */
public class Dialect {

	private final NewTableGenerator newTableHandler;
	private final NewForeignKeyGenerator newForeignKeyHandler;
	private final NewIndexGenerator newIndexHandler;
	private final DropTableGenerator dropTableHandler;
	private final ModifyColumnGenerator modifyColumnHandler;
	private final AddColumnGenerator addColumnHandler;
	private final NewUniqueConstraintGenerator newUniqueConstraintHandler;

	public Dialect() {
		this(new NewTableHandler(MARIADB_KEYWORDS),
				new NewForeignKeyHandler(),
				new NewIndexHandler(),
				new DropTableHandler(),
				new ModifyColumnHandler(MARIADB_KEYWORDS),
				new AddColumnHandler(MARIADB_KEYWORDS),
				new NewUniqueConstraintHandler(MARIADB_KEYWORDS)
				);
	}

	private Dialect(NewTableGenerator newTableHandler,
					NewForeignKeyGenerator newForeignKeyHandler,
					NewIndexGenerator newIndexHandler,
					DropTableGenerator dropTableHandler,
					ModifyColumnGenerator modifyColumnHandler,
					AddColumnGenerator addColumnHandler,
					NewUniqueConstraintGenerator newUniqueConstraintHandler
					) {
		this.newTableHandler = newTableHandler;
		this.newForeignKeyHandler = newForeignKeyHandler;
		this.newIndexHandler = newIndexHandler;
		this.dropTableHandler = dropTableHandler;
		this.modifyColumnHandler = modifyColumnHandler;
		this.addColumnHandler = addColumnHandler;
		this.newUniqueConstraintHandler = newUniqueConstraintHandler;
	}

	public String generateScript(SupportedChange supportedChange) {
		String ddl;
		if (supportedChange instanceof NewTable) {
			ddl = newTableHandler.generateScript((NewTable) supportedChange);
		} else if (supportedChange instanceof DropTable) {
			ddl = dropTableHandler.generateScript((DropTable) supportedChange);
		} else if (supportedChange instanceof NewForeignKey) {
			ddl = newForeignKeyHandler.generateScript(((NewForeignKey) supportedChange));
		} else if (supportedChange instanceof NewIndex) {
			ddl = newIndexHandler.generateScript(((NewIndex) supportedChange));
		} else if (supportedChange instanceof ModifyColumn) {
			ddl = modifyColumnHandler.generateScript(((ModifyColumn) supportedChange));
		} else if (supportedChange instanceof AddColumn) {
			ddl = addColumnHandler.generateScript(((AddColumn) supportedChange));
		} else if (supportedChange instanceof NewUniqueConstraint) {
			ddl = newUniqueConstraintHandler.generateScript(((NewUniqueConstraint) supportedChange));
		} else {
			throw new NotImplementedException("Statement of type " + Reflections.toString(supportedChange.getClass()) + " is not supported");
		}
		return ddl;
	}

	public static class DialectBuilder {

		private NewTableGenerator newTableHandler;
		private NewForeignKeyGenerator newForeignKeyHandler;
		private NewIndexGenerator newIndexHandler;
		private DropTableGenerator dropTableHandler;
		private ModifyColumnGenerator modifyColumnHandler;
		private AddColumnGenerator addColumnHandler;
		private NewUniqueConstraintGenerator newUniqueConstraintHandler;

		public DialectBuilder() {
		}

		public DialectBuilder(Dialect dialect) {
			this.newTableHandler = dialect.newTableHandler;
			this.newForeignKeyHandler = dialect.newForeignKeyHandler;
			this.newIndexHandler = dialect.newIndexHandler;
			this.dropTableHandler = dialect.dropTableHandler;
			this.modifyColumnHandler = dialect.modifyColumnHandler;
			this.addColumnHandler = dialect.addColumnHandler;
			this.newUniqueConstraintHandler = dialect.newUniqueConstraintHandler;
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
			return new Dialect(newTableHandler, newForeignKeyHandler, newIndexHandler, dropTableHandler, modifyColumnHandler, addColumnHandler, newUniqueConstraintHandler);
		}
	}
}
