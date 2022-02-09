package org.codefilarete.jumper.impl;

import java.sql.SQLException;

import org.codefilarete.jumper.AbstractChange;
import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.Context;
import org.codefilarete.jumper.ExecutionException;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.StructureDefinition;
import org.codefilarete.jumper.ddl.engine.Dialect;
import org.codefilarete.tool.exception.NotImplementedException;

/**
 * A change for any {@link StructureDefinition}
 * 
 * @author Guillaume Mary
 */
public class DDLChange extends AbstractChange {
	
	private final StructureDefinition structureDefinition;
	
	/**
	 * Constructor with mandatory arguments
	 * 
	 * @param identifier identifier of this change
	 * @param structureDefinition DDL to be run
	 */
	public DDLChange(String identifier, StructureDefinition structureDefinition) {
		super(identifier, false);	// structure changes are expected to be run once
		this.structureDefinition = structureDefinition;
	}
	
	@Override
	public void run(Context context) throws ExecutionException {
		String sql = generateScript(context.getDialect());
		try {
			context.getConnection().createStatement().execute(sql);
		} catch (SQLException e) {
			throw new ExecutionException("Can't get connection from datasource", e);
		}
	}
	
	@Override
	public Checksum computeChecksum() {
		// we ask for a footprint based on a default dialect (not tied to current targeted RDBMS) because it should be sufficient to be describing
		return StringChecksumer.INSTANCE.checksum(generateScript(new Dialect()));
	}
	
	private String generateScript(Dialect dialect) {
		if (structureDefinition instanceof NewTable) {
			return dialect.generateScript((NewTable) structureDefinition);
		} else {
			throw new NotImplementedException(structureDefinition.getClass());
		}
	}
}
