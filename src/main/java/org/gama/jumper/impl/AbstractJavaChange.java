package org.codefilarete.jumper.impl;

import org.codefilarete.jumper.AbstractChange;
import org.codefilarete.jumper.Checksum;
import org.codefilarete.jumper.ChangeId;

/**
 * An update dedicated to Java code execution.
 * Be aware that by default its Checksum depends of the class bytecode. So every code change will make it change too, hence it will considered
 * non-compliant (error will be raised) when re-applied onto an system. See {@link #computeChecksum()} for more details. 
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJavaChange extends AbstractChange {
	
	public AbstractJavaChange(ChangeId changeId, boolean shouldAlwaysRun) {
		super(changeId, shouldAlwaysRun);
	}
	
	public AbstractJavaChange(String identifier, boolean shouldAlwaysRun) {
		this(new ChangeId(identifier), shouldAlwaysRun);
	}
	
	/**
	 * Implemented to compute the Checksum of this class.
	 * Therefore it depends on this class bytecode. This makes it depends of every code modification (except comments, some spaces, line breaks, etc).
	 * Thus this principle has some drawbacks :
	 * - it doesn't take dependency to other classes into account
	 * - even code modification of non business logic (outside execute method) alters it
	 * - compiler and Java version may also alters it
	 * 
	 * So it must be considered as a best effort / minimal behavior and is far from perfect.
	 * <br/><br/>
	 * 
	 * <strong>
	 *     It is highly recommended to override this method to provide a stable checksum which should depends on what the class does.
	 *     It can be based on a description of the purpose of the class and may be changed at each modification of the business code
	 *     that has an impact on database data.
	 * </strong>
	 * 
	 * @return a {@link Checksum} of SQL orders
	 */
	@Override
	public Checksum computeChecksum() {
		return ClassChecksumer.INSTANCE.checksum(this.getClass());
	}
	
}
