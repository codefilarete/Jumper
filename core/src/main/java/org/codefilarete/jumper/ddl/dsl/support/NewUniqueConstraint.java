package org.codefilarete.jumper.ddl.dsl.support;

import org.codefilarete.jumper.impl.SupportedChange;
import org.codefilarete.tool.collection.KeepOrderSet;

import java.util.Set;

public class NewUniqueConstraint implements SupportedChange {

    private final String name;
    private final Table table;

    private final Set<String> columns = new KeepOrderSet<>();

    public NewUniqueConstraint(String name, Table table) {
        this.name = name;
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public Table getTable() {
        return table;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void addColumn(String columnName) {
        this.columns.add(columnName);
    }
}
