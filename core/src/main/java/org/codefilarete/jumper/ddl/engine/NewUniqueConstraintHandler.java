package org.codefilarete.jumper.ddl.engine;

import org.codefilarete.jumper.ddl.dsl.support.NewUniqueConstraint;
import org.codefilarete.jumper.ddl.dsl.support.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

import java.util.Set;

public class NewUniqueConstraintHandler implements NewUniqueConstraintGenerator {

    private final Set<String> keywords;

    public NewUniqueConstraintHandler(Set<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public String generateScript(NewUniqueConstraint newUniqueConstraint) {
        Table table = newUniqueConstraint.getTable();
        StringAppender sqlCreateIndex = new DDLAppender("alter table ", newUniqueConstraint.getTable().getName())
                .cat(" add constraint ", newUniqueConstraint.getName(), " unique (")
                .ccat(newUniqueConstraint.getColumns(), ", ")
                .cat(")");
        return sqlCreateIndex.toString();
    }

    /**
     * A {@link StringAppender} that automatically appends {@link Table}
     */
    private static class DDLAppender extends StringAppender {

        public DDLAppender(Object... o) {
            super(o);
        }

        /**
         * Overridden to append {@link Table} names
         *
         * @param o any object
         * @return this
         */
        @Override
        public StringAppender cat(Object o) {
            if (o instanceof Table) {
                Table table = ((Table) o);
                catIf(!Strings.isEmpty(table.getCatalogName()), table.getCatalogName(), ".");
                catIf(!Strings.isEmpty(table.getSchemaName()), table.getSchemaName(), ".");
                return super.cat(table.getName());
            } else {
                return super.cat(o);
            }
        }
    }
}
