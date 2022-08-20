package org.codefilarete.jumper.impl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.Checksumer.ByteChecksumer;
import org.codefilarete.jumper.ddl.dsl.support.DDLStatement;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;

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
			throw new NotImplementedException("Signature computation is not implemented for " + Reflections.toString(ddlStatement.getClass()));
		}
	}
	
	protected String giveSignature(NewTable newTable) {
		List<String> columnsSignature = Iterables.collect(newTable.getColumns(), this::giveSignature, ArrayList::new);
		List<String> ukSignature = Iterables.collect(newTable.getUniqueConstraints(), this::giveSignature, ArrayList::new);
		StringAppender result = new StringAppender() {
			@Override
			public StringAppender cat(Object s) {
				if (s instanceof Supplier) {
					s = ((Supplier<?>) s).get();
				}
				return super.cat(s);
			}
		};
		return result.cat("NewTable ")
				.ccat(columnsSignature, ", ")
				.cat(" ")
				.catIf(newTable.getPrimaryKey() != null, (Supplier) () -> giveSignature(newTable.getPrimaryKey()))
				.cat(" ")
				.ccat(ukSignature, ", ")
				.toString();
	}
	
	protected String giveSignature(NewTable.NewColumn newColumn) {
		StringAppender result = new StringAppender();
		return result.ccat(
						newColumn.getName(),
						newColumn.getSqlType(),
						newColumn.isNullable(),
						newColumn.getDefaultValue(),
						newColumn.isAutoIncrement(),
						newColumn.getUniqueConstraintName(),
						" ")
				.toString();
	}
	
	protected String giveSignature(NewTable.NewPrimaryKey primaryKey) {
		StringAppender result = new StringAppender();
		return result.cat("PK ").ccat(primaryKey.getColumns(), " ").toString();
	}
	
	protected String giveSignature(NewTable.NewUniqueConstraint newUniqueConstraint) {
		StringAppender result = new StringAppender();
		return result.cat("UK ", newUniqueConstraint.getName(), " ").ccat(newUniqueConstraint.getColumns(), " ").toString();
	}
	
	protected String giveSignature(DropTable ddlStatement) {
		return "DropTable " + ddlStatement.getName();
	}
	
	protected String giveSignature(NewForeignKey newForeignKey) {
		StringAppender result = new StringAppender();
		return result.cat("NewForeignKey ", newForeignKey.getName(), " ", newForeignKey.getTable().getName(), " ")
				.ccat(newForeignKey.getSourceColumns(), " ")
				.cat(", ", newForeignKey.getTargetTable().getName(), " ")
				.ccat(newForeignKey.getTargetColumns(), " ")
				.toString();
	}
	
	protected String giveSignature(NewIndex newIndex) {
		StringAppender result = new StringAppender();
		return result.cat("NewIndex ", newIndex.getName(), " ", newIndex.getTable().getName())
				.ccat(newIndex.getColumns(), " ")
				.cat(" ", newIndex.isUnique())
				.toString();
	}
}
