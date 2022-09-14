package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

/**
 *
 * @author Guillaume Mary
 */
public class DropTableHandler implements DropTableGenerator {
	
	@Override
	public String generateScript(DropTable dropTable) {
		StringAppender sqlOrder = new StringAppender("drop table ")
				// we forces target table to be on same catalog and schema that source one, because according to my knowledge no RDBMS supports
				// cross schema integrity reference, may change if one day one supports it !
				.catIf(!Strings.isEmpty(dropTable.getCatalogName()), dropTable.getCatalogName(), ".")
				.catIf(!Strings.isEmpty(dropTable.getSchemaName()), dropTable.getSchemaName(), ".")
				.cat(dropTable.getName());
		return sqlOrder.toString();
	}
}
