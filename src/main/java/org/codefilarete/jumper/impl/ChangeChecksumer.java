package org.codefilarete.jumper.impl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.Checksumer.ByteChecksumer;
import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;

import static java.lang.String.join;
import static java.lang.String.valueOf;

public class ChangeChecksumer {
	
	private final ByteChecksumer byteChecksumer = new ByteChecksumer();
	private final StringChecksumer stringChecksumer = new StringChecksumer();
	private final ClassChecksumer classChecksumer = new ClassChecksumer();
	
	public ChangeChecksumer() {
	}
	
	/**
	 * Computes the checksum of given change. Only works for well-known {@link Change} class, if not known, then a {@link NotImplementedException} is thrown.
	 * Checksum must be considered as a signature of the business logic of the update.
	 *
	 * @return a "business logic"-rely-on Checksum
	 */
	public Checksum buildChecksum(Change change) {
		Checksum result;
		if (change instanceof DDLChange) {
			DDLStatement ddlStatement = ((DDLChange) change).getDdlStatement();
			result = byteChecksumer.checksum(giveSignature(ddlStatement).getBytes(StandardCharsets.UTF_8));
		} else if (change instanceof SQLChange) {
			result = stringChecksumer.checksum(String.join(" ", ((SQLChange) change).getSqlOrders()));
		} else if (change instanceof AbstractJavaChange) {
			result = classChecksumer.checksum(change.getClass());
		} else {
			throw new NotImplementedException("Checksum computation is not implemented for " + change.getClass());
		}
		return result;
	}
	
	protected String giveSignature(DDLStatement ddlStatement) {
		if (ddlStatement instanceof NewTable) {
			return giveSignature((NewTable) ddlStatement);
		} else if (ddlStatement instanceof DropTable) {
			return giveSignature((DropTable) ddlStatement);
		} else if (ddlStatement instanceof NewForeignKey) {
			return giveSignature(((NewForeignKey) ddlStatement));
		} else if (ddlStatement instanceof NewIndex) {
			return giveSignature(((NewIndex) ddlStatement));
		} else {
			throw new NotImplementedException("Checksum computation is not implemented for " + Reflections.toString(ddlStatement.getClass()));
		}
	}
	
	protected String giveSignature(NewTable ddlStatement) {
		NewTable newTable = ddlStatement;
		List<String> columnsSignature = Iterables.collect(newTable.getColumns(), this::giveSignature, ArrayList::new);
		List<String> ukSignature = Iterables.collect(newTable.getUniqueConstraints(), this::giveSignature, ArrayList::new);
		return join(" ", "NewTable",
				join(" ", columnsSignature),
				giveSignature(newTable.getPrimaryKey()),
				join(" ", ukSignature));
	}
	
	protected String giveSignature(NewTable.NewColumn newColumn) {
		return join(" ", "NewColumn",
				newColumn.getName(),
				newColumn.getSqlType(),
				valueOf(newColumn.isNullable()),
				newColumn.getDefaultValue(),
				valueOf(newColumn.isAutoIncrement()),
				newColumn.getUniqueConstraintName());
	}
	
	protected String giveSignature(NewTable.NewPrimaryKey primaryKey) {
		return join(" ", "NewPrimaryKey", join(" ", primaryKey.getColumns()));
	}
	
	protected String giveSignature(NewTable.NewUniqueConstraint newUniqueConstraint) {
		return join(" ", "NewUniqueConstraint", newUniqueConstraint.getName(), join(" ", newUniqueConstraint.getColumns()));
	}
	
	protected static String giveSignature(DropTable ddlStatement) {
		return "drop " + ddlStatement.getName();
	}
	
	protected String giveSignature(NewForeignKey newForeignKey) {
		return join(" ", "NewForeignKey", newForeignKey.getName(),
				newForeignKey.getTable().getName(), join(" ", newForeignKey.getSourceColumns()),
				newForeignKey.getTargetTable().getName(), join(" ", newForeignKey.getTargetColumns()));
	}
	
	public String giveSignature(NewIndex newIndex) {
		return join(" ", "NewIndex", newIndex.getName(),
				newIndex.getTable().getName(),
				join(" ", newIndex.getColumns()),
				valueOf(newIndex.isUnique()));
	}
}
