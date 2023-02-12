package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.ddl.dsl.UniqueContraintCreation;

public class UniqueContraintCreationSupport implements UniqueContraintCreation {

    private final NewUniqueConstraint newUniqueConstraint;

    public UniqueContraintCreationSupport(String constraintName, String tableName, String columnName, String... extraColumnNames) {
        this.newUniqueConstraint = new NewUniqueConstraint(constraintName, new Table(tableName));
        this.newUniqueConstraint.addColumn(columnName);
        if (extraColumnNames != null) {
            for (String extraColumn : extraColumnNames) {
                newUniqueConstraint.addColumn(extraColumn);
            }
        }
    }

    @Override
    public UniqueContraintCreation setSchema(String schemaName) {
        newUniqueConstraint.getTable().setSchemaName(schemaName);
        return this;
    }

    @Override
    public UniqueContraintCreation setCatalog(String catalogName) {
        newUniqueConstraint.getTable().setCatalogName(catalogName);
        return this;
    }

    @Override
    public Change build() {
        return newUniqueConstraint;
    }
}
