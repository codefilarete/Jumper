package org.codefilarete.jumper.impl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.codefilarete.jumper.Change;
import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.Checksumer.ByteChecksumer;
import org.codefilarete.jumper.ddl.dsl.support.AddColumn;
import org.codefilarete.jumper.ddl.dsl.support.DropTable;
import org.codefilarete.jumper.ddl.dsl.support.ModifyColumn;
import org.codefilarete.jumper.ddl.dsl.support.NewForeignKey;
import org.codefilarete.jumper.ddl.dsl.support.NewIndex;
import org.codefilarete.jumper.ddl.dsl.support.NewTable;
import org.codefilarete.jumper.ddl.dsl.support.NewUniqueConstraint;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.io.IOs;

public class ChangeChecksumer {
	
	private final ByteChecksumer byteChecksumer = new ByteChecksumer();
	private final ClassChecksumer classChecksumer = new ClassChecksumer();
	
	public ChangeChecksumer() {
	}
	
	/**
	 * Computes the checksum of given change. Only works for well-known {@link ChangeSet} class, if not known, then a {@link NotImplementedException} is thrown.
	 * Checksum must be considered as a unique signature of the business logic of the update.
	 *
	 * @return a "business logic"-rely-on Checksum
	 */
	public Checksum buildChecksum(ChangeSet changes) {
		ByteBuffer byteBuffer = new ByteBuffer((int) IOs._512_Ko);
		for (Change change : changes.getChanges()) {
			if (change instanceof SupportedChange) {
				byteBuffer.append(giveSignature(((SupportedChange) change)).getBytes(StandardCharsets.UTF_8));
			} else if (change instanceof SQLChange) {
				byteBuffer.append(String.join(" ", ((SQLChange) change).getSqlOrders()).getBytes(StandardCharsets.UTF_8));
			} else if (change instanceof AbstractJavaChange) {
				byteBuffer.append(classChecksumer.buildChecksum(change.getClass()));
			} else {
				throw new NotImplementedException("Checksum computation is not implemented for " + change.getClass());
			}
		}
		return byteChecksumer.checksum(byteBuffer.getBytes());
	}
	
	protected String giveSignature(SupportedChange supportedChange) {
		if (supportedChange instanceof NewTable) {
			return giveSignature((NewTable) supportedChange);
		} else if (supportedChange instanceof DropTable) {
			return giveSignature((DropTable) supportedChange);
		} else if (supportedChange instanceof NewForeignKey) {
			return giveSignature(((NewForeignKey) supportedChange));
		} else if (supportedChange instanceof NewIndex) {
			return giveSignature(((NewIndex) supportedChange));
		} else if (supportedChange instanceof ModifyColumn) {
			return giveSignature(((ModifyColumn) supportedChange));
		} else if (supportedChange instanceof AddColumn) {
			return giveSignature(((AddColumn) supportedChange));
		} else if (supportedChange instanceof NewUniqueConstraint) {
			return giveSignature(((NewUniqueConstraint) supportedChange));
		} else {
			throw new NotImplementedException("Signature computation is not implemented for " + Reflections.toString(supportedChange.getClass()));
		}
	}
	
	protected String giveSignature(NewTable newTable) {
		List<String> columnsSignature = Iterables.collect(newTable.getColumns(), this::giveSignature, ArrayList::new);
		List<String> fkSignature = Iterables.collect(newTable.getForeignKeys(), this::giveSignature, ArrayList::new);
		List<String> ucSignature = Iterables.collect(newTable.getUniqueConstraints(), this::giveSignature, ArrayList::new);
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
				.cat(", ")
				.ccat(fkSignature, ", ")
				.cat(", ")
				.ccat(ucSignature, ", ")
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
						newColumn.isUnique(),
						" ")
				.toString();
	}
	
	protected String giveSignature(NewTable.NewPrimaryKey primaryKey) {
		StringAppender result = new StringAppender();
		return result.cat("PK ").ccat(primaryKey.getColumns(), " ").toString();
	}
	
	protected String giveSignature(NewTable.NewForeignKey newForeignKey) {
		StringAppender result = new StringAppender();
		return result.cat("FK ", newForeignKey.getReferencedTable(), " ", newForeignKey.getName(), " ")
				.ccat(newForeignKey.getColumnReferences().entrySet(), " ").toString();
	}
	
	protected String giveSignature(NewTable.NewUniqueConstraint newUniqueConstraint) {
		StringAppender result = new StringAppender();
		return result.cat("UC ", newUniqueConstraint.getName(), " ").ccat(newUniqueConstraint.getColumns(), " ").toString();
	}
	
	protected String giveSignature(DropTable dropTable) {
		return "DropTable " + dropTable.getName();
	}
	
	protected String giveSignature(NewForeignKey newForeignKey) {
		StringAppender result = new StringAppender();
		return result.cat("NewForeignKey ", newForeignKey.getName(), " ", newForeignKey.getSourceTable().getName(), " ")
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
	
	protected String giveSignature(ModifyColumn modifyColumn) {
		StringAppender result = new StringAppender("ModifyColumn");
		return result.ccat(
						modifyColumn.getTable().getName(),
						modifyColumn.getName(),
						modifyColumn.getSqlType(),
						modifyColumn.getExtraArguments(),
						modifyColumn.isNullable(),
						modifyColumn.getDefaultValue(),
						modifyColumn.isAutoIncrement(),
						" ")
				.toString();
	}
	
	protected String giveSignature(AddColumn modifyColumn) {
		StringAppender result = new StringAppender("AddColumn");
		return result.ccat(
						modifyColumn.getTable().getName(),
						modifyColumn.getName(),
						modifyColumn.getSqlType(),
						modifyColumn.getExtraArguments(),
						modifyColumn.isNullable(),
						modifyColumn.getDefaultValue(),
						modifyColumn.isAutoIncrement(),
						" ")
				.toString();
	}
	
	protected String giveSignature(NewUniqueConstraint newUniqueConstraint) {
		StringAppender result = new StringAppender();
		return result.cat("UK ", newUniqueConstraint.getName(), " ").ccat(newUniqueConstraint.getColumns(), " ").toString();
	}
	
	/**
	 * Bytes array that auto-expends if necessary appending some bytes to it
	 *
	 * @author Guillaume Mary
	 */
	static class ByteBuffer {
		
		private byte[] bytes;
		private int position = 0;
		
		ByteBuffer(int initialCapacity) {
			this.bytes = new byte[initialCapacity];
		}
		
		void append(byte[] bytes) {
			if ((this.bytes.length - position) < bytes.length) {
				byte[] newBuffer = new byte[position + bytes.length];
				System.arraycopy(this.bytes, 0, newBuffer, 0, position);
				System.arraycopy(bytes, 0, newBuffer, position, bytes.length);
				this.bytes = newBuffer;
			} else {
				System.arraycopy(bytes, 0, this.bytes, position, bytes.length);
			}
			this.position += bytes.length;
		}
		
		byte[] getBytes() {
			byte[] result = new byte[position];
			System.arraycopy(this.bytes, 0, result, 0, position);
			return result;
		}
	}
}
